#pragma once

#include <string>

#include <pthread.h>

/* Type of DML operations */
enum dml_op_type_t {
  DML_OP_TYPE_SELECT = 0,
  DML_OP_TYPE_INSERT,
  DML_OP_TYPE_UPDATE,
  DML_OP_TYPE_DELETE,
  DML_OP_TYPE_MAX,
};

/* Type of DDL operations */
enum ddl_op_type_t {
  DDL_OP_TYPE_CREATE = 0,
  DDL_OP_TYPE_DROP,
  DDL_OP_TYPE_ALTER,
  DDL_OP_TYPE_TRUNCATE,
  DDL_OP_TYPE_MAX,
};

/* Call back function definition for various DML and DDL operations */
typedef ib_err_t fn(void *);

/* to hold statistics of a particular type of operation */
struct op_err_t {
  void clear() {
    n_ops = 0;
    n_errs = 0;
    memset(errs, 0x0, sizeof(errs));
  }

  /** Total ops performed */
  int n_ops{};

  /** Total errors */
  int n_errs{};

  /** This is taken from db_err.h and it is going to be a very sparse
  array but we can live with it for testing. */
  int errs[DB_SCHEMA_NOT_LOCKED];

  /** Mutex protecting this struct. */
  pthread_mutex_t mutex;
};

/* To hold function pointers and other parameters for a table */
struct tbl_class_t {
  std::string m_name{};
  std::string m_db_name{};
  ib_tbl_fmt_t format;
  ib_ulint_t page_size;
  fn *dml_fn[DML_OP_TYPE_MAX];
  fn *ddl_fn[DDL_OP_TYPE_MAX];
};

/* Arguments to be passed to the callback functions */
struct cb_args_t {
  cb_args_t() = default;

  ib_trx_t trx{};
  int isolation_level{};
  int run_number{};
  int batch_size{};
  bool print_res{};
  op_err_t *err_st{};
  tbl_class_t *tbl{};
};

/*** Update the error stats
@param[in,out] e                Error statistics
@param[in]                      Error code.  */
void update_err_stats(op_err_t *e, ib_err_t err);
