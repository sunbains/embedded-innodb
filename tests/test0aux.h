/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

#include <stdio.h>
#include <stdlib.h>

#include "innodb.h"
#include "ut0logger.h"

constexpr int USER_OPT = 1000;

#define OK(expr)                                                               \
  do {                                                                         \
    DbErr ok_ib_err;                                                        \
    ok_ib_err = DbErr(expr);                                                  \
    if (ok_ib_err != DB_SUCCESS) {                                             \
      fprintf(stderr, "%s: %s\n", #expr, ib_strerror(ok_ib_err));              \
      exit(EXIT_FAILURE);                                                      \
    }                                                                          \
  } while (0)

#include <getopt.h>

/* InnoDB command line parameters */
extern struct option ib_longopts[];

/* Arbitrary string */
typedef struct ib_string {
  byte *ptr;
  int len;
} TestString;

/* Config variable name=value pair. */
typedef struct ib_var {
  TestString name;
  TestString value;
} TestVar;

/* Memory representation of the config file. */
typedef struct ib_config {
  TestVar *elems;
  ulint n_elems;
  ulint n_count;
} TestConfig;

/** Read a value from an integer column in an InnoDB tuple.
@return	column value */
uint64_t
read_int_from_tuple(Tuple * tpl,                  /*!< in: InnoDB tuple */
                    const ColMeta *col_meta, /*!< in: col meta data */
                    int i);                        /*!< in: column number */

/** Print all columns in a tuple. */
void print_tuple(FILE *stream,        /*!< in: Output stream */
                 const Tuple * tpl); /*!< in: Tuple to print */

/** Setup the InnoDB configuration parameters. */
void test_configure();

/** Generate random text upto max size. */
int gen_rand_text(char *ptr,     /*!< in,out: output text */
                  int max_size); /*!< in: max size of ptr */

/** Set the runtime global options.
@return DB_SUCCESS on success. */
DbErr set_global_option(int opt,          /*!< in: option index */
                           const char *arg); /*!< in: option value */

/** Print usage. */
void print_usage(const char *progname); /*!< in: name of application */

/** Print API version to stdout. */
void print_version();

/** Free the the elements. */
void config_free(TestConfig *config); /*!< in, own: config values */

/** Print the elements. */
void config_print(const TestConfig *config); /*!< in: config values */

/** Parse a config file, the file has a very simply format:
Lines beginning with '#' are ignored. Characters after '#' (inclusive)
are also ignored.  Empty lines are also ignored. Variable syntax is:
  \s*var_name\s*=\s*value\s*\n */
int config_parse_file(const char *filename, /*!< in: config file name */
                      TestConfig *config); /*!< out: config values */

/** Drop the table. */
DbErr drop_table(const char *dbname, /*!< in: database name */
                    const char *name);  /*!< in: table to drop */

/** Truncate the table. */
DbErr truncate_table(const char *dbname, /*!< in: database name */
                        const char *name);  /*!< in: table to drop */
