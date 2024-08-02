#pragma once

#include <stdio.h>
#include <stdlib.h>

#include "innodb.h"
#include "ut0logger.h"

constexpr int USER_OPT = 1000;

#define OK(expr)                                                               \
  do {                                                                         \
    ib_err_t ok_ib_err;                                                        \
    ok_ib_err = db_err(expr);                                                  \
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
  ib_byte_t *ptr;
  int len;
} ib_string_t;

/* Config variable name=value pair. */
typedef struct ib_var {
  ib_string_t name;
  ib_string_t value;
} ib_var_t;

/* Memory representation of the config file. */
typedef struct ib_config {
  ib_var_t *elems;
  ulint n_elems;
  ulint n_count;
} ib_config_t;

/** Read a value from an integer column in an InnoDB tuple.
@return	column value */
uint64_t read_int_from_tuple(
    ib_tpl_t tpl,                  /*!< in: InnoDB tuple */
    const ib_col_meta_t *col_meta, /*!< in: col meta data */
    int i);                        /*!< in: column number */

/** Print all columns in a tuple. */
void print_tuple(
    FILE *stream,        /*!< in: Output stream */
    const ib_tpl_t tpl); /*!< in: Tuple to print */

/** Setup the InnoDB configuration parameters. */
void test_configure();

/** Generate random text upto max size. */
int gen_rand_text(
    char *ptr,     /*!< in,out: output text */
    int max_size); /*!< in: max size of ptr */

/** Set the runtime global options.
@return DB_SUCCESS on success. */
ib_err_t set_global_option(
    int opt,          /*!< in: option index */
    const char *arg); /*!< in: option value */

/** Print usage. */
void print_usage(
    const char *progname); /*!< in: name of application */

/** Print API version to stdout. */
void print_version();

/** Free the the elements. */
void config_free(
    ib_config_t *config); /*!< in, own: config values */

/** Print the elements. */
void config_print(
    const ib_config_t *config); /*!< in: config values */

/** Parse a config file, the file has a very simply format:
Lines beginning with '#' are ignored. Characters after '#' (inclusive)
are also ignored.  Empty lines are also ignored. Variable syntax is:
  \s*var_name\s*=\s*value\s*\n */
int config_parse_file(
    const char *filename, /*!< in: config file name */
    ib_config_t *config); /*!< out: config values */

/** Drop the table. */
ib_err_t drop_table(
    const char *dbname, /*!< in: database name */
    const char *name);  /*!< in: table to drop */

/** Truncate the table. */
ib_err_t truncate_table(
    const char *dbname, /*!< in: database name */
    const char *name);  /*!< in: table to drop */
