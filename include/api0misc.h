/******************************************************
Interface between Innobase and client. This file contains
the functions that don't have a proper home yet.

(c) 2008 Oracle Corpn./Innobase Oy
Copyright (c) 2024 Sunny Bains. All rights reserved.

*******************************************************/

#include "innodb0types.h"
#include "os0file.h"
#include "que0que.h"
#include "trx0trx.h"

/** Determines if the currently running transaction has been interrupted.
@return	true if interrupted */
bool trx_is_interrupted(const trx_t *trx); /*!< in: transaction */

/** Create a temporary file using the OS specific function. */
int ib_create_tempfile(const char *filename); /*!< in: temp filename prefix */

/** Handles user errors and lock waits detected by the database engine.
@return	true if it was a lock wait and we should continue running the query
thread */

bool ib_handle_errors(
  enum db_err *new_err, /*!< out: possible new error
                                              encountered in lock wait, or if
                                              no new error, the value of
                                              trx->error_state at the entry of
                                              this  function */
  trx_t *trx,           /*!< in: transaction */
  que_thr_t *thr,       /*!< in: query thread */
  trx_savept_t *savept
); /*!< in: savepoint or nullptr */

/** Sets a lock on a table.
@return	error code or DB_SUCCESS */
enum db_err ib_trx_lock_table_with_retry(
  trx_t *trx,          /*!< in/out: transaction */
  Table *table, /*!< in: table to lock */
  enum Lock_mode mode
); /*!< in: lock mode */

/** Updates the table modification counter and calculates new estimates
for table and index statistics if necessary. */
void ib_update_statistics_if_needed(Table *table); /*!< in/out: table */
