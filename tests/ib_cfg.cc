/** Copyright (c) 2009 Innobase Oy. All rights reserved.
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
#include <string.h>
#include <assert.h>

#include "test0aux.h"

static void get_all(void) {
  static const char *var_names[] = {"adaptive_hash_index",
                                    "additional_mem_pool_size",
                                    "autoextend_increment",
                                    "buffer_pool_size",
                                    "checksums",
                                    "data_file_path",
                                    "data_home_dir",
                                    "doublewrite",
                                    "file_format",
                                    "file_io_threads",
                                    "file_per_table",
                                    "flush_log_at_trx_commit",
                                    "flush_method",
                                    "force_recovery",
                                    "lock_wait_timeout",
                                    "log_buffer_size",
                                    "log_file_size",
                                    "log_files_in_group",
                                    "log_group_home_dir",
                                    "max_dirty_pages_pct",
                                    "max_purge_lag",
                                    "lru_old_blocks_pct",
                                    "lru_block_access_recency",
                                    "open_files",
                                    "pre_rollback_hook",
                                    "print_verbose_log",
                                    "rollback_on_timeout",
                                    "stats_sample_pages",
                                    "status_file",
                                    "sync_spin_loops",
                                    "version",
                                    nullptr};

  const char **ptr;

  for (ptr = var_names; *ptr; ++ptr) {
    void *val;

    auto err = ib_cfg_get(*ptr, &val);
    assert(err == DB_SUCCESS);
  }
}

/** Function to test our simple config file parser. */
static void test_config_parser(void) {
  ib_config_t config;
  const char *filename = "test.conf";

  memset(&config, 0x0, sizeof(config));

  if (config_parse_file(filename, &config) == 0) {
    config_print(&config);
  } else {
    perror(filename);
  }

  config_free(&config);
}

static void test_ib_cfg_get_all(void) {
  const char **names;
  uint32_t names_num;
  char buf[8]; /* long enough to store any type */
  uint32_t i;

  /* can be called before ib_init() */
  OK(ib_cfg_get_all(&names, &names_num));

  for (i = 0; i < names_num; i++) {
    /* must be called after ib_init() */
    OK(ib_cfg_get(names[i], buf));

    /* the type of the variable can be retrieved with
    ib_cfg_var_get_type() */
  }

  free(names);
}

int main(int argc, char **argv) {
  char *ptr;
  ib_ulint_t val;
  unsigned int i;

  (void)argc;
  (void)argv;

  test_config_parser();

  auto err = ib_init();
  assert(err == DB_SUCCESS);

  test_ib_cfg_get_all();

  get_all();

  /* the value should end in / */
  err = ib_cfg_set("data_home_dir", "/some/path");
  assert(err == DB_INVALID_INPUT);

  err = ib_cfg_set("data_home_dir", "/some/path/");
  assert(err == DB_SUCCESS);

  err = ib_cfg_get("data_home_dir", &ptr);
  assert(err == DB_SUCCESS);
  assert(strcmp("/some/path/", ptr) == 0);

  err = ib_cfg_set("buffer_pool_size", 0xFFFFFFFFUL - 5);
  assert(err == DB_SUCCESS);

  err = ib_cfg_set("flush_method", "fdatasync");
  assert(err == DB_INVALID_INPUT);

  for (i = 0; i <= 100; i++) {
    err = ib_cfg_set("lru_old_blocks_pct", i);
    if (5 <= i && i <= 95) {
      assert(err == DB_SUCCESS);
      err = ib_cfg_get("lru_old_blocks_pct", &val);
      assert(err == DB_SUCCESS);
      assert(i == val);
    } else {
      assert(err == DB_INVALID_INPUT);
    }
  }

  err = ib_cfg_set("lru_block_access_recency", 123);
  assert(err == DB_SUCCESS);
  err = ib_cfg_get("lru_block_access_recency", &val);
  assert(err == DB_SUCCESS);
  assert(val == 123);

  err = ib_cfg_set("open_files", 123);
  assert(err == DB_SUCCESS);

  get_all();

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

  return (0);
}
