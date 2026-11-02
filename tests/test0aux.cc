/***************************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

#include <chrono>
#include <string_view>
#include <utility>

#include "test0aux.h"

#define COMMENT '#'

/* Runtime config */
static const char log_dir[] = "log";

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

uint64_t read_int_from_tuple(const Tuple *tpl, const ColMeta *col_meta,
                             int i) {
  uint64_t ival = 0;

  switch (col_meta->type_len) {
  case 1: {
    uint8_t v;

    auto err = tpl->copy_value(i, &v, sizeof(v));
    assert(err != IB_SQL_NULL);

    ival = (col_meta->attr & IB_COL_UNSIGNED) ? v : (int8_t)v;
    break;
  }
  case 2: {
    uint16_t v;

    auto err = tpl->copy_value(i, &v, sizeof(v));
    assert(err != IB_SQL_NULL);
    ival = (col_meta->attr & IB_COL_UNSIGNED) ? v : (int16_t)v;
    break;
  }
  case 4: {
    uint32_t v;

    auto err = tpl->copy_value(i, &v, sizeof(v));
    assert(err != IB_SQL_NULL);
    ival = (col_meta->attr & IB_COL_UNSIGNED) ? v : (int32_t)v;
    break;
  }
  case 8: {
    auto err = tpl->copy_value(i, &ival, sizeof(ival));
    assert(err != IB_SQL_NULL);
    break;
  }
  default:
    assert(false);
  }

  return (ival);
}

/** Print all columns in a tuple. */

void print_int_col(FILE *stream, const Tuple * tpl, int i,
                   ColMeta *col_meta) {
  DbErr err = DB_SUCCESS;

  switch (col_meta->type_len) {
  case 1: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      uint8_t u8;

      err = tpl->read_u8(i, &u8);
      fprintf(stream, "%u", u8);
    } else {
      int8_t i8;

      err = tpl->read_i8(i, &i8);
      fprintf(stream, "%d", i8);
    }
    break;
  }
  case 2: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      uint16_t u16;

      err = tpl->read_u16(i, &u16);
      fprintf(stream, "%u", u16);
    } else {
      int16_t i16;

      err = tpl->read_i16(i, &i16);
      fprintf(stream, "%d", i16);
    }
    break;
  }
  case 4: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      uint32_t u32;

      err = tpl->read_u32(i, &u32);
      fprintf(stream, "%u", u32);
    } else {
      int32_t i32;

      err = tpl->read_i32(i, &i32);
      fprintf(stream, "%d", i32);
    }
    break;
  }
  case 8: {
    if (col_meta->attr & IB_COL_UNSIGNED) {
      uint64_t u64;

      err = tpl->read_u64(i, &u64);
      fprintf(stream, "%llu", (unsigned long long)u64);
    } else {
      int64_t i64;

      err = tpl->read_i64(i, &i64);
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
void print_tuple(FILE *stream, const Tuple * tpl) {
  int n_cols = tpl->get_n_cols();

  for (int i = 0; i < n_cols; ++i) {
    ulint data_len;
    ColMeta col_meta;

    data_len = tpl->get_meta(i, &col_meta);

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
        DbErr err;

        err = tpl->read_float(i, &v);
        assert(err == DB_SUCCESS);
        fprintf(stream, "%f", v);
        break;
      }
      case IB_DOUBLE: {
        double v;
        DbErr err;

        err = tpl->read_double(i, &v);
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

        ptr = static_cast<const char *>(tpl->get_value(i));
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

void test_configure(Config &config) {
  using namespace std::chrono_literals;

  create_directory(log_dir);

  config.m_fil.m_flush_method = SRV_UNIX_O_DIRECT;
  config.m_fil.m_max_n_open_files = 300;
  config.m_fil.m_file_per_table = true;
  config.m_fil.m_data_home = "./";

  config.m_log.m_buffer_size = 24 * 16384;
  config.m_log.m_flush_at_trx_commit = 1;
  config.m_log.m_log_dir = log_dir;

  config.m_buf_pool.m_size = 8 * 1024 * 1024;
  config.m_buf_pool.m_use_checksums = true;

  config.m_dblwr.m_enabled = true;

  config.m_lock.m_wait_timeout = 60s;
  config.m_lock.m_rollback_on_timeout = true;

  config.m_srv.m_verbose_log = true;
}

void test_startup(System &sys, const Config &config) {
  OK(sys.init(config));
  OK(sys.startup());
}

int gen_rand_text(char *ptr, int max_size) {
  static char txt[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                      "abcdefghijklmnopqrstuvwxyz"
                      "0123456789";

  int len{};

  do {
    len = random() % max_size;
  } while (len == 0);

  for (int i = 0; i < len; ++i, ++ptr) {
    *ptr = txt[random() % (sizeof(txt) - 1)];
  }

  return (len);
}

/** The ids of the common command line options. */
enum Option : int {
  OPT_BUFFER_POOL_SIZE = 1,
  OPT_CHECKPOINT_AGE,
  OPT_IO_CAPACITY,
  OPT_LRU_OLD_RATIO,
  OPT_LRU_ACCESS_THRESHOLD,
  OPT_FORCE_RECOVERY,
  OPT_LOG_DIR,
  OPT_DATA_DIR,
  OPT_DISABLE_DBLWR,
  OPT_DISABLE_CHECKSUM,
  OPT_DISABLE_FILE_PER_TABLE,
  OPT_FLUSH_LOG_AT_TRX_COMMIT,
  OPT_FLUSH_METHOD,
  OPT_READ_THREADS,
  OPT_WRITE_THREADS,
  OPT_MAX_OPEN_FILES,
  OPT_LOCK_WAIT_TIMEOUT
};

const struct option ib_longopts[] = {
  {"ib-buffer-pool-size", required_argument, nullptr, OPT_BUFFER_POOL_SIZE},
  {"ib-checkpoint-age", required_argument, nullptr, OPT_CHECKPOINT_AGE},
  {"ib-io-capacity", required_argument, nullptr, OPT_IO_CAPACITY},
  {"ib-lru-old-ratio", required_argument, nullptr, OPT_LRU_OLD_RATIO},
  {"ib-lru-access-threshold", required_argument, nullptr, OPT_LRU_ACCESS_THRESHOLD},
  {"ib-force-recovery", required_argument, nullptr, OPT_FORCE_RECOVERY},
  {"ib-log-dir", required_argument, nullptr, OPT_LOG_DIR},
  {"ib-data-dir", required_argument, nullptr, OPT_DATA_DIR},
  {"ib-disable-dblwr", no_argument, nullptr, OPT_DISABLE_DBLWR},
  {"ib-disable-checksum", no_argument, nullptr, OPT_DISABLE_CHECKSUM},
  {"ib-disable-file-per-table", no_argument, nullptr, OPT_DISABLE_FILE_PER_TABLE},
  {"ib-flush-log-at-trx-commit", required_argument, nullptr, OPT_FLUSH_LOG_AT_TRX_COMMIT},
  {"ib-flush-method", required_argument, nullptr, OPT_FLUSH_METHOD},
  {"ib-read-threads", required_argument, nullptr, OPT_READ_THREADS},
  {"ib-write-threads", required_argument, nullptr, OPT_WRITE_THREADS},
  {"ib-max-open-files", required_argument, nullptr, OPT_MAX_OPEN_FILES},
  {"ib-lock-wait-timeout", required_argument, nullptr, OPT_LOCK_WAIT_TIMEOUT},
  {nullptr, 0, nullptr, 0}
};

/** Print usage. */
void print_usage(const char *progname) {
  fprintf(stderr,
          "usage: %s "
          "[--ib-buffer-pool-size size in mb]\n"
          "[--ib-checkpoint-age size in mb]\n"
          "[--ib-io-capacity number of records]\n"
                    "[--ib-lru-old-ratio as %% e.g. 38]\n"
          "[--ib-lru-access-threshold in ms]\n"
          "[--ib-force-recovery 1-6]\n"
          "[--ib-log-dir path]\n"
          "[--ib-data-dir path]\n"
          "[--ib-disable-dblwr]\n"
          "[--ib-disable-checksum]\n"
          "[--ib-disable-file-per-table]\n"
          "[--ib-flush-log-at-trx-commit 1-3]\n"
          "[--ib-flush-method method]\n"
          "[--ib-read-threads count]\n"
          "[--ib-write-threads count]\n"
          "[--ib-max-open-files count]\n"
          "[--ib-lock-wait-timeout seconds]\n",
          progname);
}

/**
 * @param[in] name              Name of a flush method.
 * @param[out] method           The flush method.
 *
 * @return true if name is a flush method.
 */
static bool flush_method_from_name(std::string_view name, FlushMethod &method) {
  static constexpr std::pair<std::string_view, FlushMethod> methods[] = {
    {"fsync", SRV_UNIX_FSYNC},
    {"nosync", SRV_UNIX_NOSYNC},
    {"O_DIRECT", SRV_UNIX_O_DIRECT}
  };

  for (const auto &[n, m] : methods) {
    if (n == name) {
      method = m;
      return true;
    }
  }

  return false;
}

DbErr set_global_option(Config &config, int opt, const char *arg) {
  using namespace std::chrono_literals;

  const auto number = [arg]() { return ulint(strtoul(arg, nullptr, 10)); };

  switch (opt) {
    case OPT_BUFFER_POOL_SIZE:
      config.m_buf_pool.m_size = number() * 1024UL * 1024UL;
      break;
    case OPT_CHECKPOINT_AGE:
      config.m_log.m_checkpoint_age = number() * 1024UL * 1024UL;
      break;
    case OPT_IO_CAPACITY:
      config.m_buf_pool.m_io_capacity = number();
      break;
    case OPT_LRU_OLD_RATIO:
      config.m_buf_pool.m_lru_old_blocks_pct = number();
      break;
    case OPT_LRU_ACCESS_THRESHOLD:
      config.m_buf_pool.m_lru_block_access_recency = number();
      break;
    case OPT_FORCE_RECOVERY:
      config.m_recv.m_force_recovery = IbRecovery(number());
      break;
    case OPT_LOG_DIR:
      config.m_log.m_log_dir = arg;
      break;
    case OPT_DATA_DIR:
      config.m_fil.m_data_home = arg;
      break;
    case OPT_DISABLE_DBLWR:
      config.m_dblwr.m_enabled = false;
      break;
    case OPT_DISABLE_CHECKSUM:
      config.m_buf_pool.m_use_checksums = false;
      break;
    case OPT_DISABLE_FILE_PER_TABLE:
      config.m_fil.m_file_per_table = false;
      break;
    case OPT_FLUSH_LOG_AT_TRX_COMMIT:
      config.m_log.m_flush_at_trx_commit = number();
      break;
    case OPT_FLUSH_METHOD:
      if (!flush_method_from_name(arg, config.m_fil.m_flush_method)) {
        return DB_INVALID_INPUT;
      }
      break;
    case OPT_READ_THREADS:
      config.m_aio.m_n_read_threads = number();
      break;
    case OPT_WRITE_THREADS:
      config.m_aio.m_n_write_threads = number();
      break;
    case OPT_MAX_OPEN_FILES:
      config.m_fil.m_max_n_open_files = number();
      break;
    case OPT_LOCK_WAIT_TIMEOUT:
      config.m_lock.m_wait_timeout = std::chrono::seconds(number());
      break;
    default:
      return DB_ERROR;
  }

  return DB_SUCCESS;
}

/** Print API version to stdout. */

void print_version(void) {
  auto version = api::api_version();

  printf("API: %d.%d.%d\n", (int)(version >> 32), /* Current version */
         (int)((version >> 16)) & 0xffff,         /* Revisiion */
         (int)(version & 0xffff));                /* Age */
}

/** Drop the table. */
DbErr drop_table(System &sys, const char *dbname, const char *name) {
  DbErr err;
  Trx * ib_trx;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  ib_trx = api::trx_begin(sys, IB_TRX_REPEATABLE_READ);
  assert(ib_trx != nullptr);

  err = api::schema_lock_exclusive(sys, ib_trx);
  assert(err == DB_SUCCESS);

  err = api::table_drop(sys, ib_trx, table_name);
  assert(err == DB_SUCCESS);

  err = api::trx_commit(sys, ib_trx);
  assert(err == DB_SUCCESS);

  return (err);
}

/** Truncate the table. */
DbErr truncate_table(System &sys, const char *dbname, const char *name) {
  DbErr err;
  char table_name[IB_MAX_TABLE_NAME_LEN];

  snprintf(table_name, sizeof(table_name), "%s/%s", dbname, name);

  uint64_t table_id;
  err = api::table_get_id(sys, table_name, &table_id);
  assert(err == DB_SUCCESS);

  err = api::table_truncate(sys, table_name, &table_id);
  assert(err == DB_SUCCESS);

  return (err);
}
