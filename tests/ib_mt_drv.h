#ifndef _MT_DRV_H
#define _MT_DRV_H

#include <pthread.h>

/* Type of DML operations */
typedef enum dml_op_type {
	DML_OP_TYPE_SELECT = 0,
	DML_OP_TYPE_INSERT,
	DML_OP_TYPE_UPDATE,
	DML_OP_TYPE_DELETE,
	DML_OP_TYPE_MAX,
} dml_op_type_t;

/* Type of DDL operations */
typedef enum ddl_op_type {
	DDL_OP_TYPE_CREATE = 0,
	DDL_OP_TYPE_DROP,
	DDL_OP_TYPE_ALTER,
	DDL_OP_TYPE_TRUNCATE,
	DDL_OP_TYPE_MAX,
} ddl_op_type_t;

/* Call back function definition for various DML and DDL operations */
typedef ib_err_t fn(void*);

/* to hold statistics of a particular type of operation */
typedef struct op_err_struct {
	int	n_ops;		/* Total ops performed */
	int	n_errs;		/* Total errors */
	int	errs[DB_SCHEMA_NOT_LOCKED];
				/* This is taken from db_err.h
				and it is going to be a very sparse
				array but we can live with it for
				testing. */
	pthread_mutex_t	mutex;	/*mutex protecting this struct. */
} op_err_t;

/* To hold function pointers and other parameters for a table */
typedef struct tbl_class {
	const char	name[32];
	const char	db_name[32];
	ib_tbl_fmt_t	format;
	ib_ulint_t	page_size;
	fn*		dml_fn[DML_OP_TYPE_MAX];
	fn*		ddl_fn[DDL_OP_TYPE_MAX];
} tbl_class_t;

/* Arguments to be passed to the callback functions */
typedef struct call_back_args {
	ib_trx_t	trx;
	int		isolation_level;
	int		run_number;
	int		batch_size;
	ib_bool_t	print_res;
	op_err_t*	err_st;
	tbl_class_t*	tbl;
} cb_args_t;

/**********************************************************************
Update the error stats */
void
update_err_stats(
/*=============*/
	op_err_t*	e,	/*!< in: error stat struct */
	ib_err_t	err);	/*!< in: error code */

#endif /* _MT_DRV_H */
