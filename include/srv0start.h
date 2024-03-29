/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/** @file include/srv0start.h
Starts the Innobase database server

Created 10/10/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "api0api.h"
#include "innodb0types.h"
#include "ut0byte.h"

/** Normalizes a directory path for Windows: converts slashes to backslashes. */

/** Reads the data files and their sizes from a character string.
@return	true if ok, false on parse error */
bool srv_parse_data_file_paths_and_sizes(const char *str); /** in: the data file path string */

/** Reads log group home directories from a character string.
@return	true if ok, false on parse error */
bool srv_parse_log_group_home_dirs(const char *str); /** in: character string */

/** Frees the memory allocated by srv_parse_data_file_paths_and_sizes()
and srv_parse_log_group_home_dirs(). */
void srv_free_paths_and_sizes(void);

/** Starts Innobase and creates a new database if database files
are not found and the user wants.
@return	DB_SUCCESS or error code */
ib_err_t innobase_start_or_create(void);

/** Shuts down the Innobase database.
@return	DB_SUCCESS or error code */
enum db_err innobase_shutdown(ib_shutdown_t shutdown); /** in: shutdown flag */

/** Log sequence number at shutdown */
extern uint64_t srv_shutdown_lsn;

/** Log sequence number immediately after startup */
extern uint64_t srv_start_lsn;

#ifdef HAVE_DARWIN_THREADS
/** true if the F_FULLFSYNC option is available */
extern bool srv_have_fullfsync;
#endif

/** true if the server is being started */
extern bool srv_is_being_started;

/** true if the server was successfully started */
extern bool srv_was_started;

/** true if the server is being started, before rolling back any
incomplete transactions */
extern bool srv_startup_is_before_trx_rollback_phase;

/** true if a raw partition is in use */
extern bool srv_start_raw_disk_in_use;

/** Shutdown state */
enum srv_shutdown_state {
  /** Database running normally */
  SRV_SHUTDOWN_NONE = 0,

  /** Cleaning up in logs_empty_and_mark_files_at_shutdown() */
  SRV_SHUTDOWN_CLEANUP,

  /** Last phase after ensuring that the buffer pool can be freed:
  flush all file spaces and close all files */
  SRV_SHUTDOWN_LAST_PHASE,

  /** Exit all threads */
  SRV_SHUTDOWN_EXIT_THREADS
};

/** At a shutdown this value climbs from SRV_SHUTDOWN_NONE to
SRV_SHUTDOWN_CLEANUP and then to SRV_SHUTDOWN_LAST_PHASE, and so on */
extern srv_shutdown_state srv_shutdown_state;

/** Log 'spaces' have id's >= this */
constexpr ulint SRV_LOG_SPACE_FIRST_ID = 0xFFFFFFF0UL;
