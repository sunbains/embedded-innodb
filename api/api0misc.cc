/** Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith

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

#ifdef __WIN__

#include <io.h>

/* Required for mapping file system errors */
void __cdecl _dosmaperr(unsigned long);

/* The length was chosen arbitrarily and should be generous. */
#define WIN_MAX_PATH 512

/** @file api/api0misc.c
Create a temporary file in Windows.
@return	file descriptor, or -1 */
static int
ib_win_create_tempfile(const char *prefix) /*!< in: temp file prefix */
{
  ulint ret;
  TCHAR path[WIN_MAX_PATH];
  TCHAR tempname[WIN_MAX_PATH];

  int fh = -1;

  ret = GetTempPath(sizeof(path), path);

  if (ret > sizeof(path) || ret == 0) {
    _dosmaperr(GetLastError());
  } else if (!GetTempFileName(path, prefix, 0, tempname)) {
    _dosmaperr(GetLastError());
  } else {
    HANDLE handle;

    /* Create the actual file with the relevant attributes. */
    handle =
        CreateFile(tempname, GENERIC_READ | GENERIC_WRITE | DELETE,
                   FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                   CREATE_ALWAYS,
                   FILE_ATTRIBUTE_NORMAL | FILE_FLAG_DELETE_ON_CLOSE |
                       FILE_ATTRIBUTE_TEMPORARY | FILE_FLAG_SEQUENTIAL_SCAN,
                   NULL);

    if (handle == INVALID_HANDLE_VALUE) {
      _dosmaperr(GetLastError());
    } else {
      do {
        DWORD oserr;

        fh = _open_osfhandle((intptr_t)handle, 0);

        /* We need to map the Windows OS error
        number (if any) to errno. */
        oserr = GetLastError();
        if (oserr) {
          _dosmaperr(oserr);
        }
      } while (fh == -1 && errno == EINTR);

      if (fh == -1) {
        _dosmaperr(GetLastError());
        CloseHandle(handle);
      }
    }
  }

  return (fh);
}
#endif

/** Create a temporary file. FIXME: This is a Q&D solution. */

int ib_create_tempfile(const char *prefix) /*!< in: temp filename prefix */
{
  int fh = -1;

#ifdef __WIN__
  fh = ib_win_create_tempfile(prefix);
#else
  FILE *file;

  (void)prefix;

  file = tmpfile();

  if (file != NULL) {
    fh = dup(fileno(file));
    fclose(file);
  }
#endif

  return (fh);
}

/** Determines if the currently running transaction has been interrupted.
@return	true if interrupted */

ib_trx_is_interrupted_handler_t ib_trx_is_interrupted = NULL;

bool trx_is_interrupted(const trx_t *trx) /*!< in: transaction */
{
  if (trx->client_thd && ib_trx_is_interrupted != NULL) {
    return (ib_trx_is_interrupted(trx->client_thd));
  }
  return (false);
}

/** Handles user errors and lock waits detected by the database engine.
@return	true if it was a lock wait and we should continue running the query
thread */

bool ib_handle_errors(
    enum db_err *new_err, /*!< out: possible new error encountered in
                          lock wait, or if no new error, the value
                          of trx->error_state at the entry of this
                          function */
    trx_t *trx,           /*!< in: transaction */
    que_thr_t *thr,       /*!< in: query thread */
    trx_savept_t *savept) /*!< in: savepoint or NULL */
{
  enum db_err err;

handle_new_error:
  err = trx->error_state;

  ut_a(err != DB_SUCCESS);

  trx->error_state = DB_SUCCESS;

  switch (err) {
  case DB_LOCK_WAIT_TIMEOUT:
    if (ses_rollback_on_timeout) {
      trx_general_rollback(trx, false, NULL);
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
    srv_suspend_user_thread(thr);

    if (trx->error_state != DB_SUCCESS) {
      que_thr_stop_client(thr);

      goto handle_new_error;
    }

    *new_err = err;

    return (true); /* Operation needs to be retried. */

  case DB_DEADLOCK:
  case DB_LOCK_TABLE_FULL:
    /* Roll back the whole transaction; this resolution was added
    to version 3.23.43 */

    trx_general_rollback(trx, false, NULL);
    break;

  case DB_MUST_GET_MORE_FILE_SPACE:
    log_fatal("InnoDB: The database cannot continue"
              " operation because of\n"
              "InnoDB: lack of space. You must add"
              " a new data file\n"
              "InnoDB: and restart the database.\n");
    break;

  case DB_CORRUPTION:
    ib_logger(ib_stream, "InnoDB: We detected index corruption"
                         " in an InnoDB type table.\n"
                         "InnoDB: You have to dump + drop + reimport"
                         " the table or, in\n"
                         "InnoDB: a case of widespread corruption,"
                         " dump all InnoDB\n"
                         "InnoDB: tables and recreate the"
                         " whole InnoDB tablespace.\n"
                         "InnoDB: If the server crashes"
                         " after the startup or when\n"
                         "InnoDB: you dump the tables, check the \n"
                         "InnoDB: InnoDB website for help.\n");
    break;
  default:
    ib_logger(ib_stream, "InnoDB: unknown error code %lu\n", (ulong)err);
    ut_error;
  }

  if (trx->error_state != DB_SUCCESS) {
    *new_err = trx->error_state;
  } else {
    *new_err = err;
  }

  trx->error_state = DB_SUCCESS;

  return (false);
}

/** Sets a lock on a table.
@return	error code or DB_SUCCESS */

enum db_err
ib_trx_lock_table_with_retry(trx_t *trx,          /*!< in/out: transaction */
                             dict_table_t *table, /*!< in: table to lock */
                             enum lock_mode mode) /*!< in: LOCK_X or LOCK_S */
{
  que_thr_t *thr;
  enum db_err err;
  mem_heap_t *heap;
  sel_node_t *node;

  ut_ad(trx->client_thread_id == os_thread_get_curr_id());

  heap = mem_heap_create(512);

  trx->op_info = "setting table lock";

  node = sel_node_create(heap);
  thr = pars_complete_graph_for_exec(node, trx, heap);
  thr->graph->state = QUE_FORK_ACTIVE;

  /* We use the select query graph as the dummy graph needed
  in the lock module call */

  thr = que_fork_get_first_thr(
      static_cast<que_fork_t *>(que_node_get_parent(thr)));
  que_thr_move_to_run_state(thr);

run_again:
  thr->run_node = thr;
  thr->prev_node = thr->common.parent;

  err = lock_table(0, table, mode, thr);

  trx->error_state = err;

  if (likely(err == DB_SUCCESS)) {
    que_thr_stop_for_client_no_error(thr, trx);
  } else {
    que_thr_stop_client(thr);

    if (err != DB_QUE_THR_SUSPENDED) {
      bool was_lock_wait;

      was_lock_wait = ib_handle_errors(&err, trx, thr, NULL);

      if (was_lock_wait) {
        goto run_again;
      }
    } else {
      que_thr_t *run_thr;
      que_node_t *parent;

      parent = que_node_get_parent(thr);
      run_thr = que_fork_start_command(static_cast<que_fork_t *>(parent));

      ut_a(run_thr == thr);

      /* There was a lock wait but the thread was not
      in a ready to run or running state. */
      trx->error_state = DB_LOCK_WAIT;

      goto run_again;
    }
  }

  que_graph_free(thr->graph);
  trx->op_info = "";

  return (err);
}

/** Updates the table modification counter and calculates new estimates
for table and index statistics if necessary. */

void ib_update_statistics_if_needed(dict_table_t *table) /*!< in/out: table */
{
  ulint counter;

  counter = table->stat_modified_counter++;

  /* Calculate new statistics if 1 / 16 of table has been modified
  since the last time a statistics batch was run, or if
  stat_modified_counter > 2 000 000 000 (to avoid wrap-around).
  We calculate statistics at most every 16th round, since we may have
  a counter table which is very small and updated very often. */

  if (counter > 2000000000 ||
      ((int64_t)counter > 16 + table->stat_n_rows / 16)) {

    dict_update_statistics(table);
  }
}
