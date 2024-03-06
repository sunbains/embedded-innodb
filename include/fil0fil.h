/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.

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

/** @file include/fil0fil.h
The low-level file system

Created 10/25/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "dict0types.h"
#include "fil0types.h"
#include "os0file.h"
#include "srv0srv.h"
#include "sync0rw.h"
#include "ut0byte.h"

/** Returns the version number of a tablespace, -1 if not found.
@return version number, -1 if the tablespace does not exist in the
memory cache
@param[in] id                   Tablespace ID */
int64_t fil_space_get_version(ulint id);

/** Returns the latch of a file space.
@param[in] id                   Tablespace ID
@return	latch protecting storage allocation */
rw_lock_t *fil_space_get_latch(ulint id);

/** Returns the type of a file space.
@param[in] id                   Tablespace ID
@return	FIL_TABLESPACE or FIL_LOG */
ulint fil_space_get_type(ulint id);

/** Appends a new file to the chain of files of a space. File must be closed.
@param[in] name                File name (file must be closed)
@param[in] size                File size in database blocks, rounded
                               downwards to an integer
@param[in] id                  Tablespace id where to append
@param[in] is_raw              true if a raw device or a raw disk partition */
void fil_node_create(const char *name, ulint size, ulint id, bool is_raw);

/** Creates a space memory object and puts it to the 'fil system' hash table. If
there is an error, prints an error message to the .err log.
@param[in] name                 Tablespace name
@param[in] id                   Tablespace ID
@param[in] flags                Tablespace flags
@param[in] purpose              FIL_TABLESPACE, or FIL_LOG if log
@return	true if success */
bool fil_space_create(const char *name, ulint id, ulint flags, ulint purpose);

/** Returns the size of the space in pages. The tablespace must be cached in the
memory cache.
@param[in] id                   Tablespace ID
@return	space size, 0 if space not found */
ulint fil_space_get_size(ulint id);

/** Returns the flags of the space. The tablespace must be cached
in the memory cache.
@param[in] id                   Tablespace ID
@return	flags, ULINT_UNDEFINED if space not found */
ulint fil_space_get_flags(ulint id);

/** Returns the compressed page size of the space, or 0 if the space
is not compressed. The tablespace must be cached in the memory cache.
@param[in] id                   Tablespace ID
@return	compressed page size, ULINT_UNDEFINED if space not found */
ulint fil_space_get_size(ulint id);

/** Checks if the pair space, page_no refers to an existing page in a tablespace
file space. The tablespace must be cached in the memory cache.
@param[in] id                   Tablespace ID
@param[in] page_no              Page number in id
@return	true if the address is meaningful */
bool fil_check_adress_in_tablespace(ulint id, ulint page_no);

/** Initializes the tablespace memory cache.
@param[in] hash_size            Hash tbale size
@param[in] max_n_open           Max number of open files. */
void fil_init(ulint hash_size, ulint max_n_open);

/** Deinitializes the tablespace memory cache. */
void fil_close();

/** Opens all log files and system tablespace data files. They stay open until
the database server shutdown. This should be called at a server startup after
the space objects for the log and the system tablespace have been created. The
purpose of this operation is to make sure we never run out of file descriptors
if we need to read from the insert buffer or to write to the log. */
void fil_open_log_and_system_tablespace_files();

/** Closes all open files. There must not be any pending i/o's or not flushed
modifications in the files. */
void fil_close_all_files();

/** Sets the max tablespace id counter if the given number is bigger than the
previous value.
@param[in] max_id               Maximum known tablespace ID.  */
void fil_set_max_space_id_if_bigger(ulint max_id);

/** Writes the flushed lsn and the latest archived log number to the page
header of the first page of each data file in the system tablespace.
@param[in] lsn                  LSN to write.
@return	DB_SUCCESS or error number */
db_err fil_write_flushed_lsn_to_data_files(lsn_t lsn);

/** Reads the flushed lsn and arch no fields from a data file at database
startup.
@param[in] data_file             Open data file handle
@param[in] one_read_only         true if min and max parameters below contain
                                 sensible data.
@param[out] min_arch_log_no      Minmum log number
@param[out] max_arch_log_no      Maximum log number
@param[out] min_flushed_lsn      Minimum flushed LSN.
@param[out] max_flushed_lsn      Maximum flushed LSN */
void fil_read_flushed_lsn_and_arch_log_no(os_file_t data_file,
                                          bool one_read_already,
                                          uint64_t *min_flushed_lsn,
                                          uint64_t *max_flushed_lsn);

/** Increments the count of pending insert buffer page merges, if space is not
being deleted.
@param[in] id                   Tablespace ID
@return	true if being deleted, and ibuf merges should be skipped */
bool fil_inc_pending_ibuf_merges(ulint id);

/** Decrements the count of pending insert buffer page merges.
@param[in] id                   Tablespace ID */
void fil_decr_pending_ibuf_merges(ulint id);

/** Parses the body of a log record written about an .ibd file operation. That
is, the log record part after the standard (type, space id, page no) header of
the log record.

If desired, also replays the delete or rename operation if the .ibd file
exists and the space id in it matches. Replays the create operation if a file
at that path does not exist yet. If the database directory for the file to be
created does not exist, then we create the directory, too.

Note that ibbackup --apply-log sets fil_path_to_client_datadir to point to the
datadir that we should use in replaying the file operations.
@return end of log record, or NULL if the record was not completely
contained between ptr and end_ptr

@param[in] ptr                  buffer containing the log record body,
                                or an initial segment of it.
@param[in] end_ptr              Buffer end
@param[in] type                 The type of this log record.
@param[in] space_id             The space id of the tablespace in question, or
                                0 if the log record should only be parsed but
                                not replayed
@param[in] log_flags            redo log flags (stored in the page number
                                parameter) */
byte *fil_op_log_parse_or_replay(byte *ptr, byte *end_ptr, ulint type,
                                 ulint space_id, ulint log_flags);

/** Deletes a single-table tablespace. The tablespace must be cached in the
memory cache.
@param[in] id                   Tablespace ID
@return	true if success */
bool fil_delete_tablespace(ulint id);

/** Discards a single-table tablespace. The tablespace must be cached in the
memory cache. Discarding is like deleting a tablespace, but

  1. We do not drop the table from the data dictionary;
  2. We remove all insert buffer entries for the tablespace immediately; in DROP
     TABLE they are only removed gradually in the background;
  3. When the user does IMPORT TABLESPACE, the tablespace will have the same id
     as it originally had.

@param[in] id                   Tablespace ID
@return	true if success */
bool fil_discard_tablespace(ulint id);

/** Renames a single-table tablespace. The tablespace must be cached in the
tablespace memory cache.

@param[in] old_name             Old table name in the standard
                                databasename/tablename format of
                                InnoDB, or NULL if we do the rename
                                based on the space id only
@param[in] id                   Tablepace id
@param[in] new_name             New table name in the standard
                                databasename/tablename format of InnoDB

@return	true if success */
bool fil_rename_tablespace(const char *old_name, ulint id,
                           const char *new_name);

/** Creates a new single-table tablespace in a database directory.
The datadir is the current directory of a running program. We can
refer to it by simply the path '.'. Tables created with:
      CREATE TEMPORARY TABLE
we place in the configured TEMP dir of the application.

@param[in,out] space_id,        Space id; if this is != 0, then this is an
                                input parameter, otherwise output
@param[in] tablename            The table name in the usual
                                databasename/tablename format of InnoDB,
                                or a dir path to a temp table
@param[in] is_temp              true if a table created with
                                CREATE TEMPORARY TABLE
@param[in] flags                Tablespace flags
@param[in] size);               The initial size of the tablespace file
                                in pages, must be >= FIL_IBD_FILE_INITIAL_SIZE
@return	DB_SUCCESS or error code */
db_err fil_create_new_single_table_tablespace(ulint *space_id,
                                              const char *tablename,
                                              bool is_temp, ulint flags,
                                              ulint size);

/** Tries to open a single-table tablespace and optionally checks the space id
is right in it. If does not succeed, prints an error message to the .err log.
This function is used to open a tablespace when we start up the application, and
also in IMPORT TABLESPACE.
NOTE that we assume this operation is used either at the database startup
or under the protection of the dictionary mutex, so that two users cannot
race here. This operation does not leave the file associated with the
tablespace open, but closes it after we have looked at the space id in it.

@return	true if success */
bool fil_open_single_table_tablespace(
    bool check_space_id, /** in: should we check that the space
                          id in the file is right; we assume
                          that this function runs much faster
                          if no check is made, since accessing
                          the file inode probably is much
                          faster (the OS caches them) than
                          accessing the first page of the file */
    ulint id,            /** in: space id */
    ulint flags,         /** in: tablespace flags */
    const char *name);   /** in: table name in the
                         databasename/tablename format */

/** It is possible, though very improbable, that the lsn's in the tablespace to
be imported have risen above the current system lsn, if a lengthy purge, ibuf
merge, or rollback was performed on a backup taken with ibbackup. If that is
the case, reset page lsn's in the file. We assume that the engine was shutdown
after it performed these cleanup operations on the .ibd file, so that it at
the shutdown stamped the latest lsn to the FIL_PAGE_FILE_FLUSH_LSN in the
first page of the .ibd file, and we can determine whether we need to reset the
lsn's just by looking at that flush lsn.
@return	true if success */
bool fil_reset_too_high_lsns(
    const char *name,      /** in: table name in the
                           databasename/tablename format */
    uint64_t current_lsn); /** in: reset lsn's if the lsn stamped
                              to FIL_PAGE_FILE_FLUSH_LSN in the
                              first page is too high */

/** At the server startup, if we need crash recovery, scans the database
directories under the current dir, looking for .ibd files. Those files are
single-table tablespaces. We need to know the space id in each of them so that
we know into which file we should look to check the contents of a page stored
in the doublewrite buffer, also to know where to apply log records where the
space id is != 0.
@return	DB_SUCCESS or error number */
db_err fil_load_single_table_tablespaces(
    ib_recovery_t recovery); /** in: recovery flag */

/** If we need crash recovery, and we have called
fil_load_single_table_tablespaces() and dict_load_single_table_tablespaces(),
we can call this function to print an error message of orphaned .ibd files
for which there is not a data dictionary entry with a matching table name
and space id. */
void fil_print_orphaned_tablespaces(void);

/** Returns true if a single-table tablespace does not exist in the memory
cache, or is being deleted there.
@return	true if does not exist or is being deleted */
bool fil_tablespace_deleted_or_being_deleted_in_mem(
    ulint id,         /** in: space id */
    int64_t version); /** in: tablespace_version should be this; if
                        you pass -1 as the value of this, then this
                        parameter is ignored */

/** Returns true if a single-table tablespace exists in the memory cache.
@return	true if exists */
bool fil_tablespace_exists_in_mem(ulint id); /** in: space id */

/** Returns true if a matching tablespace exists in the InnoDB tablespace memory
cache. Note that if we have not done a crash recovery at the database startup,
there may be many tablespaces which are not yet in the memory cache.
@return	true if a matching tablespace exists in the memory cache */
bool fil_space_for_table_exists_in_mem(
    ulint id,         /** in: space id */
    const char *name, /** in: table name in the standard
                      'databasename/tablename' format or
                      the dir path to a temp table */
    bool is_temp,     /** in: true if created with CREATE
                       TEMPORARY TABLE */
    bool mark_space,  /** in: in crash recovery, at database
                       startup we mark all spaces which have
                       an associated table in the InnoDB
                       data dictionary, so that
                       we can print a warning about orphaned
                       tablespaces */
    bool print_error_if_does_not_exist);

/** Tries to extend a data file so that it would accommodate the number of pages
given. The tablespace must be cached in the memory cache. If the space is big
enough already, does nothing.
@return	true if success */
bool fil_extend_space_to_desired_size(
    ulint *actual_size,       /** out: size of the space after extension;
                              if we ran out of disk space this may be lower
                              than the desired size */
    ulint space_id,           /** in: space id */
    ulint size_after_extend); /** in: desired size in pages after the
                           extension; if the current space size is bigger
                           than this already, the function does nothing */

/** Tries to reserve free extents in a file space.
@return	true if succeed */
bool fil_space_reserve_free_extents(
    ulint id,            /** in: space id */
    ulint n_free_now,    /** in: number of free extents now */
    ulint n_to_reserve); /** in: how many one wants to reserve */

/** Releases free extents in a file space. */
void fil_space_release_free_extents(
    ulint id,          /** in: space id */
    ulint n_reserved); /** in: how many one reserved */

/** Gets the number of reserved extents. If the database is silent, this number
should be zero. */
ulint fil_space_get_n_reserved_extents(ulint id); /** in: space id */

/** Reads or writes data. This operation is asynchronous (aio).
@return DB_SUCCESS, or DB_TABLESPACE_DELETED if we are trying to do
i/o on a tablespace which does not exist */
db_err fil_io(ulint type,         /** in: OS_FILE_READ or OS_FILE_WRITE,
                                 ORed to OS_FILE_LOG, if a log i/o
                                 and ORed to OS_AIO_SIMULATED_WAKE_LATER
                                 if simulated aio and we want to post a
                                 batch of i/os; NOTE that a simulated batch
                                 may introduce hidden chances of deadlocks,
                                 because i/os are not actually handled until
                                 all have been posted: use with great
                                 caution! */
              bool sync,         /** in: true if synchronous aio is desired */
              space_id_t space_id,/** in: space id */
              ulint block_offset, /** in: offset in number of blocks */
              ulint byte_offset,  /** in: remainder of offset in bytes; in
                                  aio this must be divisible by the OS block
                                  size */
              ulint len,          /** in: how many bytes to read or write; this
                                  must not cross a file boundary; in aio this
                                  must be a block size multiple */
              void *buf,          /** in/out: buffer where to store read data
                                  or from where to write; in aio this must be
                                  appropriately aligned */
              void *message);     /** in: message for aio handler if non-sync
                                  aio used, else ignored */

/** Waits for an aio operation to complete. This function is used to write the
handler for completed requests. The aio array of pending requests is divided
into segments (see os0file.c for more info). The thread specifies which
segment it wants to wait for. */
void fil_aio_wait(ulint segment); /** in: the number of the segment in the aio
                                  array to wait for */

/** Flushes to disk possible writes cached by the OS. If the space does not
exist or is being dropped, does not do anything. */
void fil_flush(ulint space_id); /** in: file space id (this can be a group of
                                log files or a tablespace of the database) */

/** Flushes to disk writes in file spaces of the given type possibly cached by
the OS. */
void fil_flush_file_spaces(ulint purpose); /** in: FIL_TABLESPACE, FIL_LOG */

/** Checks the consistency of the tablespace cache.
@return	true if ok */
bool fil_validate();

/** Returns true if file address is undefined.
@return	true if undefined */
bool fil_addr_is_null(fil_addr_t addr); /** in: address */

/** Get the predecessor of a file page.
@return	FIL_PAGE_PREV */
ulint fil_page_get_prev(const byte *page); /** in: file page */

/** Get the successor of a file page.
@return	FIL_PAGE_NEXT */
ulint fil_page_get_next(const byte *page); /** in: file page */

/** Sets the file page type. */
void fil_page_set_type(byte *page,  /** in/out: file page */
                       ulint type); /** in: type */

/** Gets the file page type.
@return type; NOTE that if the type has not been written to page, the
return value not defined */
ulint fil_page_get_type(const byte *page); /** in: file page */

/** Reset variables. */
void fil_var_init();

/** Remove the underlying directory where the database .ibd files are stored.
@return	true on success */
bool fil_rmdir(const char *dbname); /** in: database name */

/** Create the underlying directory where the database .ibd files are stored.
@return	true on success */
bool fil_mkdir(const char *dbname); /** in: database name */

