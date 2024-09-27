/** Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith
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
#include "ib0config.h"

#include <errno.h>

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif /* HAVE_UNISTD_H */

#include "api0misc.h"
#include "dict0dict.h"
#include "dict0mem.h"
#include "innodb0types.h"
#include "lock0lock.h"
#include "pars0pars.h"
#include "row0sel.h"
#include "srv0srv.h"
#include "trx0roll.h"

/* If set then we rollback the transaction on DB_LOCK_WAIT_TIMEOUT error. */
bool ses_rollback_on_timeout = false;

int ib_create_tempfile(const char *) {
  int fh = -1;
  auto file = tmpfile();

  if (file != nullptr) {
    fh = dup(fileno(file));
    fclose(file);
  }

  return fh;
}

/** Determines if the currently running transaction has been interrupted.
@return	true if interrupted */
ib_trx_is_interrupted_handler_t ib_trx_is_interrupted = nullptr;

bool trx_is_interrupted(const trx_t *trx) {
  if (trx->m_client_ctx && ib_trx_is_interrupted != nullptr) {
    return ib_trx_is_interrupted(trx->m_client_ctx);
  }
  return false;
}

bool ib_handle_errors(db_err *new_err, trx_t *trx, que_thr_t *thr, trx_savept_t *savept) {
  for (;;) {
    auto err = trx->error_state;
    ut_a(err != DB_SUCCESS);

    trx->error_state = DB_SUCCESS;

    switch (err) {
      case DB_LOCK_WAIT_TIMEOUT:
        if (ses_rollback_on_timeout) {
          trx_general_rollback(trx, false, nullptr);
          break;
        }
        /* fall through */
      case DB_DUPLICATE_KEY:
      case DB_FOREIGN_DUPLICATE_KEY:
      case DB_TOO_BIG_RECORD:
      case DB_ROW_IS_REFERENCED:
      case DB_NO_REFERENCED_ROW:
      case DB_CANNOT_ADD_CONSTRAINT:
      case DB_TOO_MANY_CONCURRENT_TRXS:
      case DB_OUT_OF_FILE_SPACE:
        if (savept) {
          /* Roll back the latest, possibly incomplete
          insertion or update */

          trx_general_rollback(trx, true, savept);
        }
        break;
      case DB_LOCK_WAIT:
        InnoDB::suspend_user_thread(thr);

        if (trx->error_state != DB_SUCCESS) {
          que_thr_stop_client(thr);

          continue;
        }

        *new_err = err;

         /* Operation needs to be retried. */
        return true;

      case DB_DEADLOCK:
      case DB_LOCK_TABLE_FULL:
        /* Roll back the whole transaction; this resolution was added
        to version 3.23.43 */

        trx_general_rollback(trx, false, nullptr);
        break;

      case DB_MUST_GET_MORE_FILE_SPACE:
        log_fatal(
          "The database cannot continue operation because of"
          " lack of space. You must add a new data file and restart"
          " the database."
        );
        break;

      case DB_CORRUPTION:
        log_err(
          "We detected index corruption in an InnoDB type table."
          " You have to dump + drop + reimport the table or, in"
          " a case of widespread corruption, dump all InnoDB tables"
          " and recreate the whole InnoDB tablespace. If the server"
          " crashes after the startup or when you dump the tables,"
          " check the InnoDB website for help.");
        break;
      default:
        log_fatal("Unknown error code ", (ulint) err);
    }

    if (trx->error_state != DB_SUCCESS) {
      *new_err = trx->error_state;
    } else {
      *new_err = err;
    }

    trx->error_state = DB_SUCCESS;

    return false;
  }
}

db_err ib_trx_lock_table_with_retry(trx_t *trx, dict_table_t *table, enum Lock_mode mode) {
  auto heap = mem_heap_create(512);

  trx->m_op_info = "setting table lock";

  auto node = sel_node_create(heap);
  auto thr = pars_complete_graph_for_exec(node, trx, heap);

  thr->graph->state = QUE_FORK_ACTIVE;

  /* We use the select query graph as the dummy graph needed
  in the lock module call */

  thr = que_fork_get_first_thr(static_cast<que_fork_t *>(que_node_get_parent(thr)));
  que_thr_move_to_run_state(thr);

  for (;;) {
    thr->run_node = thr;
    thr->prev_node = thr->common.parent;

    auto err = srv_lock_sys->lock_table(0, table, mode, thr);

    trx->error_state = err;

    if (likely(err == DB_SUCCESS)) {
      que_thr_stop_for_client_no_error(thr, trx);
    } else {
      que_thr_stop_client(thr);

      if (err != DB_QUE_THR_SUSPENDED) {
        auto was_lock_wait = ib_handle_errors(&err, trx, thr, nullptr);

        if (was_lock_wait) {
          continue;
        }
      } else {
        auto parent = que_node_get_parent(thr);
        auto run_thr = que_fork_start_command(static_cast<que_fork_t *>(parent));

        ut_a(run_thr == thr);

        /* There was a lock wait but the thread was not
        in a ready to run or running state. */
        trx->error_state = DB_LOCK_WAIT;

        continue;
      }
    }

    que_graph_free(thr->graph);
    trx->m_op_info = "";

    return err;
  }

   ut_error;
}

void ib_update_statistics_if_needed(dict_table_t *table) {
  auto counter = table->stat_modified_counter++;

  /* Calculate new statistics if 1 / 16 of table has been modified
  since the last time a statistics batch was run, or if
  stat_modified_counter > 2 000 000 000 (to avoid wrap-around).
  We calculate statistics at most every 16th round, since we may have
  a counter table which is very small and updated very often. */

  if (counter > 2000000000 || ((int64_t)counter > 16 + table->stat_n_rows / 16)) {

    dict_update_statistics(table);
  }
}

const char *ib_strerror(ib_err_t err) {
  switch (err) {
    case DB_SUCCESS:
      return "Success";
    case DB_PANIC:
      return "Panic";
    case DB_ERROR:
      return "Generic error";
    case DB_OUT_OF_MEMORY:
      return "Cannot allocate memory";
    case DB_OUT_OF_FILE_SPACE:
      return "Out of disk space";
    case DB_LOCK_WAIT:
      return "Lock wait";
    case DB_DEADLOCK:
      return "Deadlock";
    case DB_ROLLBACK:
      return "Rollback";
    case DB_DUPLICATE_KEY:
      return "Duplicate key";
    case DB_QUE_THR_SUSPENDED:
      return "The queue thread has been suspended";
    case DB_MISSING_HISTORY:
      return "Required history data has been deleted";
    case DB_CLUSTER_NOT_FOUND:
      return "Cluster not found";
    case DB_TABLE_NOT_FOUND:
      return "Table not found";
    case DB_MUST_GET_MORE_FILE_SPACE:
      return "More file space needed";
    case DB_TABLE_EXISTS:
      return "Table is being used";
    case DB_TOO_BIG_RECORD:
      return "Record too big";
    case DB_LOCK_WAIT_TIMEOUT:
      return "Lock wait timeout";
    case DB_NO_REFERENCED_ROW:
      return "Referenced key value not found";
    case DB_ROW_IS_REFERENCED:
      return "Row is referenced";
    case DB_CANNOT_ADD_CONSTRAINT:
      return "Cannot add constraint";
    case DB_CORRUPTION:
      return "Data structure corruption";
    case DB_COL_APPEARS_TWICE_IN_INDEX:
      return "Column appears twice in index";
    case DB_CANNOT_DROP_CONSTRAINT:
      return "Cannot drop constraint";
    case DB_NO_SAVEPOINT:
      return "No such savepoint";
    case DB_TABLESPACE_ALREADY_EXISTS:
      return "Tablespace already exists";
    case DB_TABLESPACE_DELETED:
      return "No such tablespace";
    case DB_LOCK_TABLE_FULL:
      return "Lock structs have exhausted the buffer pool";
    case DB_FOREIGN_DUPLICATE_KEY:
      return "Foreign key activated with duplicate keys";
    case DB_TOO_MANY_CONCURRENT_TRXS:
      return "Too many concurrent transactions";
    case DB_UNSUPPORTED:
      return "Unsupported";
    case DB_PRIMARY_KEY_IS_NULL:
      return "Primary key is nullptr";
    case DB_FAIL:
      return "Failed, retry may succeed";
    case DB_OVERFLOW:
      return "Overflow";
    case DB_UNDERFLOW:
      return "Underflow";
    case DB_STRONG_FAIL:
      return "Failed, retry will not succeed";
    case DB_RECORD_NOT_FOUND:
      return "Record not found";
    case DB_END_OF_INDEX:
      return "End of index";
    case DB_SCHEMA_ERROR:
      return "Error while validating a table or index schema";
    case DB_DATA_MISMATCH:
      return "Type mismatch";
    case DB_SCHEMA_NOT_LOCKED:
      return "Schema not locked";
    case DB_NOT_FOUND:
      return "Not found";
    case DB_READONLY:
      return "Readonly";
    case DB_INVALID_INPUT:
      return "Invalid input";
    case DB_FATAL:
      return "InnoDB fatal error";
    case DB_INTERRUPTED:
      return "Operation interrupted";
    case DB_OUT_OF_RESOURCES:
      return "Out of resources";
    case DB_INDEX_CORRUPT:
      return "Index corrupt";
    case DB_DDL_IN_PROGRESS:
      return "DDL in progress";
  }

  /* Do not add default: in order to produce a warning if new code
  is added to the enum but not added here */

  /* NOT REACHED */
  return "Unknown error";
}
