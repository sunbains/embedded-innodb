/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

/** @file include/db0err.h
Error codes for the database

Created 5/24/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include <format>

/** \enum DbErr InnoDB error codes.
 * Most of the error codes are internal to the engine
 * and will not be seen by user applications. The partial error codes reflect
 * the sub-state of an operation within InnoDB. Some of the error codes are
 * deprecated and are no longer used. */
enum DbErr {
  /** A successult result */
  DB_SUCCESS = 10,    

  /* We can't proceed, internal error. */
  DB_PANIC,

  /* The following are error codes */

  /** This is a generic error code. It is used to classify
  error conditions that can't be represented by other codes */
  DB_ERROR,      

  /** An operation was interrupted by a user. */
  DB_INTERRUPTED,      

  /** Operation caused an out of memory error. Within InnoDB
  core code this is normally a fatal error */
  DB_OUT_OF_MEMORY,    

  /** The operating system returned an out of file space error
   when trying to do an IO operation. */
  DB_OUT_OF_FILE_SPACE,    

  /** A lock request by transaction resulted in a lock wait.
  The thread is suspended internally by InnoDB and is put on a
         lock wait queue. */
  DB_LOCK_WAIT,      

  /** A lock request by a transaction resulted in a deadlock.
  The transaction was rolled back */
  DB_DEADLOCK,      

  /*!< Not used */
  DB_ROLLBACK,      

  /** A record insert or update violates a unique contraint. */
  DB_DUPLICATE_KEY,    
  
  /** A query thread should be in state suspended but is
  trying to acquire a lock. Currently this is treated as a hard
  error and a violation of an invariant. */
  DB_QUE_THR_SUSPENDED,    

  /** Required history data has been deleted due to lack of space
  in rollback segment */
  DB_MISSING_HISTORY,    

  /** This error is not used */
  DB_CLUSTER_NOT_FOUND = 30,

  /** The table could not be found */
  DB_TABLE_NOT_FOUND,    

  /** The database has to be stopped and restarted with more
  file space */
  DB_MUST_GET_MORE_FILE_SPACE,  

  /** The user is trying to create a table in the InnoDB data
  dictionary but a table with that name already exists */
  DB_TABLE_EXISTS,    

  /** A record in an index would not fit on a compressed page,
  or it would become bigger than 1/2 free space in an uncompressed
  page frame */
  DB_TOO_BIG_RECORD,    

  /** Lock wait lasted too long */
  DB_LOCK_WAIT_TIMEOUT,    

  /** Referenced key value not found fonnic_r a foreign key in an
  insert or update of a row */
  DB_NO_REFERENCED_ROW,    

  /** Cannot delete or update a row because it contains a key
  value which is referenced */
  DB_ROW_IS_REFERENCED,    

  /** Adding a foreign key constraint to a table failed */
  DB_CANNOT_ADD_CONSTRAINT,  

  /** Data structure corruption noticed */
  DB_CORRUPTION,      

  /** InnoDB cannot handle an index where same column appears
  twice */
  DB_COL_APPEARS_TWICE_IN_INDEX,  

  /** Dropping a foreign key constraint from a table failed */
  DB_CANNOT_DROP_CONSTRAINT,  

  /** No savepoint exists with the given name */
  DB_NO_SAVEPOINT,    

  /** We cannot create a new single-table tablespace because a
  file of the same name already exists */
  DB_TABLESPACE_ALREADY_EXISTS,  

  /** Tablespace does not exist or is being dropped right now */
  DB_TABLESPACE_DELETED,    

  /** Lock structs have exhausted the buffer pool (for big
  transactions, InnoDB stores the lock structs in the buffer pool) */
  DB_LOCK_TABLE_FULL,    

  /** Foreign key constraints activated but the operation would
  lead to a duplicate key in some table */
  DB_FOREIGN_DUPLICATE_KEY,  

  /** When InnoDB runs out of the preconfigured undo slots,
  this can only happen when there are too many concurrent transactions */
  DB_TOO_MANY_CONCURRENT_TRXS,  

  /** When InnoDB sees any artefact or a feature that it can't
  recoginize or work with e.g., FT indexes created by a later version of the engine. */
  DB_UNSUPPORTED,      

  /** A column in the PRIMARY KEY was found to be nullptr */
  DB_PRIMARY_KEY_IS_NULL,    

  /** The application should clean up and quite ASAP. Fatal error,
  InnoDB cannot continue operation without risking database corruption. */
  DB_FATAL,      

  /* The following are partial failure codes */

  /** Partial failure code. */
  DB_FAIL = 1000,

  /** If an update or insert of a record doesn't fit in a Btree page */
  DB_OVERFLOW,      

  /** If an update or delete of a record causes a Btree page
  to be below a minimum threshold */
  DB_UNDERFLOW,      

  /** Failure to insert a secondary index entry to the insert buffer */
  DB_STRONG_FAIL,      

  /** Record not found */
  DB_RECORD_NOT_FOUND = 1500,

  /** A cursor operation or search operation scanned to the end
  of the index. */
  DB_END_OF_INDEX,    

  /* The following are API only error codes. */

  /** Generic schema error */
  DB_SCHEMA_ERROR = 2000,

  /** Column update or read failed because the types mismatch */
  DB_DATA_MISMATCH,    

  /** If an API function expects the schema to be locked in
  exclusive mode and if it's not then that API function will
  return this error code */
  DB_SCHEMA_NOT_LOCKED,    

  /** Generic error code for "Not found" type of errors */
  DB_NOT_FOUND,      

  /** Generic error code for "Readonly" type of errors */
  DB_READONLY,      

  /** Generic error code for "Invalid input" type of errors */
  DB_INVALID_INPUT,    

  /** Out of a resource, could be memory, threads, file descriptors etc. */
  DB_OUT_OF_RESOURCES,

  /** Index is corrupted. */
  DB_INDEX_CORRUPT,

  /** Ddl is in progress. */
  DB_DDL_IN_PROGRESS,
};


/** std::formatter specialization for DbErr to enable use with std::format */
template <>
struct std::formatter<DbErr> : std::formatter<int> {
  auto format(DbErr e, std::format_context& ctx) const {
    return std::formatter<int>::format(static_cast<int>(e), ctx);
  }
};

/* Basic types used by the InnoDB API. */

/** @return a human readable description of the error code. */
[[nodiscard]] const char *db_strerror(DbErr err) noexcept;
