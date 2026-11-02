/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

/** @file tests/test0aux.h
Helpers shared by the test programs. */

#pragma once

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#include "api0api.h"
#include "db0err.h"
#include "srv0cfg.h"
#include "srv0sys.h"
#include "ut0logger.h"

using api::Col;
using api::Cursor;
using api::IndexSchema;
using api::KeyCol;
using api::SchemaVisitor;
using api::TableSchema;
using api::TableStats;
using api::Tuple;

using api::IB_MAX_COL_NAME_LEN;
using api::IB_MAX_TABLE_NAME_LEN;
using api::IB_N_SYS_COLS;
using api::IB_SQL_NULL;
using api::MAX_TEXT_LEN;

/** The first option id a test program can use for its own options. */
constexpr int USER_OPT = 1000;

#define OK(expr)                                                  \
  do {                                                            \
    const auto ok_ib_err = DbErr(expr);                           \
    if (ok_ib_err != DB_SUCCESS) {                                \
      fprintf(stderr, "%s: %s\n", #expr, db_strerror(ok_ib_err)); \
      exit(EXIT_FAILURE);                                         \
    }                                                             \
  } while (0)

/** The command line options common to all the test programs, terminated by
an all zero entry. */
extern const struct option ib_longopts[];

/**
 * Read a value from an integer column in an InnoDB tuple.
 *
 * @param[in] tpl               InnoDB tuple.
 * @param[in] col_meta          Column meta data.
 * @param[in] i                 Column number.
 *
 * @return column value
 */
[[nodiscard]] uint64_t read_int_from_tuple(const Tuple *tpl, const ColMeta *col_meta, int i);

/**
 * Print all columns in a tuple.
 *
 * @param[in] stream            Output stream.
 * @param[in] tpl               Tuple to print.
 */
void print_tuple(FILE *stream, const Tuple *tpl);

/**
 * Set the configuration the test programs run with.
 *
 * @param[out] config           The configuration to set.
 */
void test_configure(Config &config);

/**
 * Configure, initialize and start an instance, exits on failure.
 *
 * @param[in,out] sys           The instance to start.
 * @param[in] config            The configuration to start it with.
 */
void test_startup(System &sys, const Config &config);

/**
 * Generate random text upto max size.
 *
 * @param[out] ptr              Output text.
 * @param[in] max_size          Max size of ptr.
 *
 * @return the length of the text.
 */
int gen_rand_text(char *ptr, int max_size);

/**
 * Set a configuration option from the command line.
 *
 * @param[in,out] config        The configuration to change.
 * @param[in] opt               Option id, from ib_longopts.
 * @param[in] arg               Option value.
 *
 * @return DB_SUCCESS on success.
 */
[[nodiscard]] DbErr set_global_option(Config &config, int opt, const char *arg);

/**
 * Print usage.
 *
 * @param[in] progname          Name of the application.
 */
void print_usage(const char *progname);

/** Print the API version to stdout. */
void print_version();

/**
 * Drop the table.
 *
 * @param[in] sys               The instance.
 * @param[in] dbname            Database name.
 * @param[in] name              Table to drop.
 *
 * @return DB_SUCCESS or error code
 */
DbErr drop_table(System &sys, const char *dbname, const char *name);

/**
 * Truncate the table.
 *
 * @param[in] sys               The instance.
 * @param[in] dbname            Database name.
 * @param[in] name              Table to truncate.
 *
 * @return DB_SUCCESS or error code
 */
DbErr truncate_table(System &sys, const char *dbname, const char *name);
