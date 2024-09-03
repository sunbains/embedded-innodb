/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
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

/** @file include/fil0fil.h
The low-level file system

Created 10/25/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "dict0types.h"
#include "fil0types.h"
#include "mtr0types.h"
#include "os0aio.h"
#include "os0file.h"
#include "srv0srv.h"
#include "sync0rw.h"

#include <unordered_map>

// Forward declaration
struct mtr_t;

struct Fil {

  /** Constructor
   *  @param[in] max_n_open Maxium number of open files
   */
  explicit Fil(ulint max_n_open);

  /** Destructor */
  ~Fil() noexcept;

  /** Returns the version number of a tablespace, -1 if not found.
  @return version number, -1 if the tablespace does not exist in the
  memory cache
  @param[in] space_id             Tablespace ID */
  int64_t space_get_version(space_id_t space_id);

  /** Returns the latch of a file space.
  @param[in] space_id             Tablespace ID
  @return	latch protecting storage allocation */
  rw_lock_t *space_get_latch(space_id_t space_id);

  /** Returns the type of a file space.
  @param[in] space_id             Tablespace ID
  @return	FIL_TABLESPACE or FIL_LOG */
  ulint space_get_type(space_id_t space_id);

  /** Appends a new file to the chain of files of a space. File must be closed.
  @param[in] name                File name (file must be closed)
  @param[in] size                File size in database blocks, rounded
                                       downwards to an integer
  @param[in] space_id            Tablespace id where to append
  @param[in] is_raw              true if a raw device or a raw disk partition */
  void node_create(const char *name, ulint size, space_id_t space_id, bool is_raw);

  /** Creates a space memory object and puts it to the 'fil system' hash map. If
  there is an error, prints an error message to the .err log.
  @param[in] name                 Tablespace name
  @param[in] space_id             Tablespace ID
  @param[in] flags                Tablespace flags
  @param[in] purpose              FIL_TABLESPACE, or FIL_LOG if log
  @return	true if success */
  bool space_create(const char *name, space_id_t space_id, ulint flags, Fil_type type);

  /** Returns the size of the space in pages. The tablespace must be cached in the
  memory cache.
  @param[in] space_id             Tablespace ID
  @return	space size, 0 if space not found */
  ulint space_get_size(space_id_t space_id);

  /** Returns the flags of the space. The tablespace must be cached
  in the memory cache.
  @param[in] space_id             Tablespace ID
  @return	flags, ULINT_UNDEFINED if space not found */
  ulint space_get_flags(space_id_t id);

  /** Checks if the pair space, page_no refers to an existing page in a tablespace
  file space. The tablespace must be cached in the memory cache.
  @param[in] space_id             Tablespace ID
  @param[in] page_no              Page number in id
  @return	true if the address is meaningful */
  bool check_adress_in_tablespace(space_id_t space_id, page_no_t page_no);

  /** Opens all log files and system tablespace data files. They stay open until
  the database server shutdown. This should be called at a server startup after
  the space objects for the log and the system tablespace have been created. The
  purpose of this operation is to make sure we never run out of file descriptors
  if we need to read from the insert buffer or to write to the log. */
  void open_log_and_system_tablespace();

  /** Closes all open files. There must not be any pending i/o's or not flushed
  modifications in the files. */
  void close_all_files();

  /** Sets the max tablespace id counter if the given number is bigger than the
  previous value.
  @param[in] max_id               Maximum known tablespace ID.  */
  void set_max_space_id_if_bigger(space_id_t max_id);

  /** Writes the flushed lsn and the latest archived log number to the page
  header of the first page of each data file in the system tablespace.
  @param[in] lsn                  LSN to write.
  @return	DB_SUCCESS or error number */
  db_err write_flushed_lsn_to_data_files(lsn_t lsn);

  /** Reads the flushed lsn and arch no fields from a data file at database
  startup.
  @param[in] fh                    Open data file handle
  @param[out] flushed_lsn          Maximum flushed LSN */
  void read_flushed_lsn(os_file_t fh, lsn_t &max_flushed_lsn);

   /** Parses the body of a log record written about an .ibd file operation. That
   is, the log record part after the standard (type, space id, page no) header of
   the log record.

   If desired, also replays the delete or rename operation if the .ibd file
   exists and the space id in it matches. Replays the create operation if a file
   at that path does not exist yet. If the database directory for the file to be
   created does not exist, then we create the directory, too.

   Note that ibbackup --apply-log sets path_to_client_datadir to point to the
   datadir that we should use in replaying the file operations.
   @return end of log record, or nullptr if the record was not completely
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
   byte *op_log_parse_or_replay(
     byte *ptr,
     byte *end_ptr,
     ulint type,
     space_id_t space_id,
     ulint log_flags);

   /** Deletes a single-table tablespace. The tablespace must be cached in the
   memory cache.
   @param[in] space_id             Tablespace ID
   @return	true if success */
   bool delete_tablespace(space_id_t space_id);

   /** Discards a single-table tablespace. The tablespace must be cached in the
   memory cache. Discarding is like deleting a tablespace, but

   1. We do not drop the table from the data dictionary;
   2. We remove all insert buffer entries for the tablespace immediately; in DROP
      TABLE they are only removed gradually in the background;
   3. When the user does IMPORT TABLESPACE, the tablespace will have the same id
      as it originally had.

   @param[in] id                   Tablespace ID
   @return	true if success */
   bool discard_tablespace(ulint id);

   /** Renames a single-table tablespace. The tablespace must be cached in the
   tablespace memory cache.

   @param[in] old_name             Old table name in the standard
                                   databasename/tablename format of
                                   InnoDB, or nullptr if we do the rename
                                   based on the space id only
   @param[in] space_id             Tablepace id
   @param[in] new_name             New table name in the standard
                                   databasename/tablename format of InnoDB

   @return	true if success */
   bool rename_tablespace(const char *old_name, space_id_t space_id, const char *new_name);

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
  db_err create_new_single_table_tablespace(
    space_id_t *space_id,
    const char *tablename,
    bool is_temp,
    ulint flags,
    ulint size);

  /**
   * Tries to open a single-table tablespace and optionally checks the space id
   * is right in it. If does not succeed, prints an error message to the .err log.
   * This function is used to open a tablespace when we start up the application, and
   * also in IMPORT TABLESPACE.
   * NOTE that we assume this operation is used either at the database startup
   * or under the protection of the dictionary mutex, so that two users cannot
   * race here. This operation does not leave the file associated with the
   * tablespace open, but closes it after we have looked at the space id in it.
   *
   * @param[in] check_space_id   should we check that the space id in the file is right;
   *                             we assume that this function runs much faster if no check
   *                             is made, since accessing the file inode probably is much
   *                             faster (the OS caches them) than accessing the first page of the file
   * @param[in] space_id         space id
   * @param[in] flags            tablespace flags
   * @param[in] name             table name in the databasename/tablename format
   *
   * @return true if success
   */
  bool open_single_table_tablespace(bool check_space_id, space_id_t space_id, ulint flags, const char *name);

  /**
   * At the server startup, if we need crash recovery, scans the database
   * directories under the specified dir, looking for .ibd files. Those files are
   * single-table tablespaces. We need to know the space id in each of them so that
   * we know into which file we should look to check the contents of a page stored
   * in the doublewrite buffer, also to know where to apply log records where the
   * space id is != 0.
   *
   * @param[in] path              The path to the database files and sub-directories
   * @param[in] recovery          The recovery flag
   * @param[in] max_depth         The maximum depth of the directory tree to scan
   *
   * @return  DB_SUCCESS or error number
   */
  db_err load_single_table_tablespaces(const std::string &path, ib_recovery_t recovery, size_t max_depth);

  /** If we need crash recovery, and we have called
  load_single_table_tablespaces() and dict_load_single_table_tablespaces(),
  we can call this function to print an error message of orphaned .ibd files
  for which there is not a data dictionary entry with a matching table name
  and space id. */
  void print_orphaned_tablespaces();

  /**
   * Returns true if a single-table tablespace does not exist in the memory
   * cache, or is being deleted there.
   *
   * @param[in] space_id          Space id
   * @param[in] version           Tablespace version; if you pass -1 as the value of this,
   *  then this parameter is ignored
   *
   * @return true if does not exist or is being deleted
   */
  bool tablespace_deleted_or_being_deleted_in_mem(space_id_t space_id, int64_t version);

  /**
   * Returns true if a single-table tablespace exists in the memory cache.
   *
   * @param[in] Space_id    Space id
   *
   * @return true if exists
   */
  bool tablespace_exists_in_mem(space_id_t space_id);

  /**
   * Returns true if a matching tablespace exists in the InnoDB tablespace memory
   * cache. Note that if we have not done a crash recovery at the database startup,
   * there may be many tablespaces which are not yet in the memory cache.
   *
   * @param[in] space_id          space id
   * @param[in] name              table name in the standard 'databasename/tablename'
   *                              format or the dir path to a temp table
   * @param[in] is_temp           true if created with CREATE TEMPORARY TABLE
   * @param[in] mark_space        in crash recovery, at database startup we mark all
   *                              spaces which have an associated table in the InnoDB
   *                              data dictionary, so that we can print a warning about
   *                              orphaned tablespaces
   * @param[in] print_error_if_does_not_exist
   *
   * @return true if a matching tablespace exists in the memory cache
   */
  bool space_for_table_exists_in_mem(
    space_id_t id,
    const char *name,
    bool is_temp,
    bool mark_space,
    bool print_error_if_does_not_exist);

  /**
   * Tries to extend a data file so that it would accommodate
   * the number of pages given. The tablespace must be cached
   * in the memory cache. If the space is big enough already, does nothing.
   *
   * @param[out]actual_size       Size of the space after extension;
   *                              if we ran out of disk space this may be
   *                              lower than the desired size
   * @param[in] space_id          space id
   * @param[in] size_after_extend desired size in pages after the extension;
   *                              if the current space size is bigger than this
   *                              already, the function does nothing
   *
   * @return true if success
   */
  bool extend_space_to_desired_size(ulint *actual_size, space_id_t space_id, ulint size_after_extend);

  /**
   * Tries to reserve free extents in a file space.
   *
   * @param[in] space_id          Space id
   * @param[in] n_free_now        Number of free extents now
   * @param[in] n_to_reserve      How many one wants to reserve
   *
   * @return true if succeed
   */
  bool space_reserve_free_extents(space_id_t space_id, ulint n_free_now, ulint n_to_reserve);

  /**
   * Releases free extents in a file space.
   *
   * @param[in] space_id          Space id
   * @param[in] n_reserved        How many one reserved
   */
  void space_release_free_extents(space_id_t id, ulint n_reserved);

  /* *
   * Gets the number of reserved extents. If the database is silent, this number should be zero.
   *
   * @param[in] space_id          Space id
   *
   * @return Number of reserved extents
   */
  ulint space_get_n_reserved_extents(space_id_t space_id);

  /**
   * Reads or writes data. This operation is asynchronous (aio).
   * @param io_request            in: IO_request type.
   * @param batched               in: if simulated aio and we want to post a
   *                              batch of i/os; NOTE that a simulated batch
   *                              may introduce hidden chances of deadlocks,
   *                              because i/os are not actually handled until
   *                              all have been posted: use with great
   *                              caution!
   * @param space_id              in: space id
   * @param block_offset          in: offset in number of blocks
   * @param byte_offset           in: remainder of offset in bytes; in
   *                              aio this must be divisible by the OS block size
   * @param len                   in: how many bytes to read or write; this
   *                              must not cross a file boundary; in aio this
   *                              must be a block size multiple
   * @param buf                   in/out: buffer where to store read data
   *                              or from where to write; in aio this must be
   *                              appropriately aligned
   * @param message               in: message for aio handler if non-sync
   *                             aio used, else ignored
   * @return DB_SUCCESS, or DB_T ABLESPACE_DELETED if we are trying to do
   *         i/o on a tablespace which does not exist
   */
  db_err io(
    IO_request io_request,
    bool batched,
    space_id_t space_id,
    page_no_t page_no,
    ulint byte_offset,
    ulint len,
    void *buf,
    void *message);

  /**
   * Waits for an aio operation to complete. This function is used to write the
   * handler for completed requests. The aio array of pending requests is divided
   * into segments (see os0file.c for more info). The thread specifies which
   * segment it wants to wait for.
   * 
   * @param[in] segment           The number of the segmentto wait for
   * @return false if AIO reaper was shutdown
   */
  bool aio_wait(ulint segment);

  /**
   * Flushes to disk possible writes cached by the OS. If the space does not
   * exist or is being dropped, does not do anything.
   * 
   * @param[in] space_id          File space id (this can be a group of log files or
   *  a tablespace of the database)
   */
  void flush(space_id_t space_id);

  /**
   * Flushes to disk writes in file spaces of the given type possibly cached by
   * the OS.
   * 
   * @param[in] purpose           FIL_TABLESPACE, FIL_LOG
   */
  void flush_file_spaces(ulint purpose);

  /**
   * Checks the consistency of the tablespace cache.
   * 
   * @return true if ok
   */
  bool validate();

  /**
   * Returns true if file address is undefined.
   * 
   * @param[in] addr              Address
   * @return true if undefined
   */
  bool addr_is_null(const fil_addr_t& addr);

  /**
   * Get the predecessor of a file page.
   * 
   * @param[in] page - file page
   * @return FIL_PAGE_PREV
   */
  ulint page_get_prev(const byte *page);

  /**
   * Get the successor of a file page.
   * 
   * @param[in] page               File page
   * @return FIL_PAGE_NEXT
   */
  ulint page_get_next(const byte *page);

  /**
   * Sets the file page type.
   * 
   * @param[in,out] page          File page
   * @param[in] type - type
   */
  static void page_set_type(byte *page, ulint type);

  /**
   * Gets the file page type.
   * @param[in] page              File page
   * @return type; NOTE that if the type has not been written to page, the
   *         return value not defined
   */
  static Fil_page_type page_get_type(const byte *page);

  /**
   * Reset variables.
   */ 
  void var_init();

  /**
   * Remove the underlying directory where the database .ibd files are stored.
   * @param[in] dbname - database name
   * @return true on success
   */
  bool rmdir(const char *dbname); /** in: database name */

  /**
   * Create the underlying directory where the database .ibd files are stored.
   * @param[in] dbname - database name
   * @return true on success
   */
  bool mkdir(const char *dbname); /** in: database name */

  /** @return Number of pending redo log flushes */
  ulint get_pending_log_flushes() const {
    return m_n_pending_log_flushes;
  }

  /** @return Number of pending tablespace flushes */
  ulint get_pending_tablespace_flushes() const {
    return m_n_pending_tablespace_flushes;
  }

  /** @return The number of fsyncs done to the log */
  ulint get_log_flushes() const {
    return m_n_log_flushes;
  }

private:
  /**
   * @brief Frees a space object from the tablespace memory cache. Closes the files in
   * the chain but does not delete them. There must not be any pending i/o's or
   * flushes on the files.
   * 
   * @param id - in: space id
   * @param own_mutex - in: true if own m_mutex
   * @return true if success
   */
  bool space_free(space_id_t id, bool own_mutex);

  /**
   * @brief Remove extraneous '.' && '\' && '/' characters from the prefix.
   * 
   * Note: Currently it will not handle paths like: ../a/b.
   * 
   * @param ptr - in: path to normalize
   * @return pointer to normalized path
   */
  const char *normalize_path(const char *ptr);

  /**
   * @brief Compare two table names.
   * 
   * It will compare the two table names using the canonical names.
   * e.g., ./a/b == a/b. TODO: /path/to/a/b == a/b if both /path/to/a/b and a/b refer to the same file.
   * 
   * @param name1 - in: table name to compare
   * @param name2 - in: table name to compare
   * 
   * @return = 0 if name1 == name2 < 0 if name1 < name2 > 0 if name1 > name2
   */
  int tablename_compare(const char *name1, const char *name2);

  /**
   * @brief Updates the data structures when an i/o operation finishes.
   * 
   * @param node - file node
   * @param system - tablespace memory cache
   * @param io_request - IO_request::Write or IO_request::Read; marks the node as
   *  modified if type == IO_request::Write
   */
  void node_complete_io(fil_node_t *node, IO_request io_request);

  /**
   * @brief Checks if a single-table tablespace for a given table name
   * exists in the tablespace memory cache.
   * 
   * @param name - table name in the standard 'databasename/tablename' format
   * @return space id, ULINT_UNDEFINED if not found
   */
  ulint get_space_id_for_table(const char *name);

  /**
   * @brief Returns the table space by a given id, nullptr if not found.
   * 
   * @param id space id
   * @return Fil::fil_space_t* table space, nullptr if not found
   */
  fil_space_t *space_get_by_id(space_id_t id);

  /**
   * @brief Returns the table space by a given name, nullptr if not found.
   * 
   * @param name in: space name
   * @return Fil::fil_space_t* table space, nullptr if not found
   */
  fil_space_t *space_get_by_name(const char *name);

  /**
  * @brief Checks if all the file nodes in a space are flushed.
  * 
  * @param space in: space
  * @return true if all are flushed
  */
  bool space_is_flushed(fil_space_t *space) ;

  /**
   * Opens a the file of a node of a tablespace.
   * The caller must own the Fil::system mutex.
   *
   * @param node The file node.
   * @param space The space.
   * @param[in] startup true if the server is starting up
   */
  void node_open_file(fil_node_t *node, fil_space_t *space, bool startup);

  /**
   * @brief Closes a file.
   *
   * @param node in: file node
   * @param system in: tablespace memory cache
   */
  void node_close_file(fil_node_t *node);

  /**
   * @brief Reserves the Fil::system mutex and tries to make sure we can open at least
   * one file while holding it. This should be called before calling
   * Fil::node_prepare_for_io(), because that function may need to open a file.
   *
   * @param space_id in: space id
   */
  void mutex_enter_and_prepare_for_io(space_id_t space_id);

  /**
  * @brief Frees a file node object from a tablespace memory cache.
  *
  * @param node in, own: file node
  * @param space in: space where the file node is chained
  */
  void node_free(fil_node_t *node, fil_space_t *space);

  /** Assigns a new space id for a new single-table tablespace. This works simply
  by incrementing the global counter. If 4 billion id's is not enough, we may need
  to recycle id's.
  @return	new tablespace id; ULINT_UNDEFINED if could not assign an id */
  ulint assign_new_space_id();

  /**
  * @brief Writes the flushed lsn and the latest archived log number to the page header
  * of the first page of a data file of the system tablespace (space 0),
  * which is uncompressed.
  *
  * @param sum_of_sizes in: combined size of previous files in space, in database pages
  * @param lsn in: lsn to write
  *
  * @return db_err
  */
  db_err write_lsn_and_arch_no_to_file(ulint sum_of_sizes, lsn_t lsn);

  /**
  * @brief Creates the database directory for a table if it does not exist yet.
  *
  * @param name in: name in the standard 'databasename/tablename' format
  */
  void create_directory_for_tablename(const char *name);

  /**
  * @brief Writes a log record about an .ibd file create/rename/delete.
  *
  * @param type      in: MLOG_FILE_CREATE, MLOG_FILE_CREATE2, MLOG_FILE_DELETE, or MLOG_FILE_RENAME
  * @param space_id  in: space id
  * @param log_flags in: redo log flags (stored in the page number field)
  * @param flags     in: compressed page size and file format if type==MLOG_FILE_CREATE2, or 0
  * @param name      in: table name in the familiar 'databasename/tablename' format, or the file path in the case of MLOG_FILE_DELETE
  * @param new_name  in: if type is MLOG_FILE_RENAME, the new table name in the 'databasename/tablename' format
  * @param mtr       in: mini-transaction handle
  */
  void op_write_log(
    mlog_type_t type,
    space_id_t space_id,
    ulint log_flags,
    ulint flags,
    const char *name,
    const char *new_name,
    mtr_t *mtr
  );

  /**
   * Renames a tablespace in memory.
   *
   * This function renames the specified tablespace in memory. It takes a tablespace memory object,
   * a file node, and a new name as input parameters. The function updates the name of the tablespace
   * in memory to the new name provided.
   *
   * @param space The tablespace memory object to be renamed.
   * @param node The file node of the tablespace.
   * @param path The new name for the tablespace.
   * @return true if the tablespace was successfully renamed, false otherwise.
   */
  bool rename_tablespace_in_mem(fil_space_t *space, fil_node_t *node, const char *path);

  /**
  * @brief Allocates a file name for a single-table tablespace.
  * The string must be freed by caller with mem_free().
  * 
  * @param name The table name or a dir path of a TEMPORARY table.
  * @param is_temp True if it is a dir path.
  * @return char* The allocated file name.
  */
  char *make_ibd_name(const char *name, bool is_temp);

  /**
  * @param recovery recovery flag
  * @param dbname database (or directory) name
  * @param filename file name (not a path), including the .ibd extension
  */
  void load_single_table_tablespace(ib_recovery_t recovery, const char *dbname, const char *filename);

  /** Scan the given directory and load the tablespaces. So that we can map the physical files back
   * to their names that are stored in the data dictionary.
   * 
   * @param dir The directory to scan
   * @param recovery The recovery flag
   * @param max_depth The maximum depth of the directory tree to scan
   * @param depth The current depth of the directory tree 
   */
  db_err scan_and_load_tablespaces(const std::string &dir, ib_recovery_t recovery, ulint max_depth, ulint depth);

  /**
  * @brief Prepares a file node for i/o. Opens the file if it is closed. Updates the
  * pending i/o's field in the node and the system appropriately. Takes the node
  * off the LRU list if it is in the LRU list. The caller must hold the Fil::sys
  * mutex.
  *
  * @param node in: file node
  * @param space in: space
  */
  void node_prepare_for_io(fil_node_t *node, fil_space_t *space);

  /**
  * @brief Report information about an invalid page access.
  *
  * @param block_offset   in: block offset
  * @param space_id       in: space id
  * @param space_name     in: space name
  * @param byte_offset    in: byte offset
  * @param len            in: I/O length
  * @param io_request     in: I/O request type
  */
  [[noreturn]] void report_invalid_page_access(
    ulint block_offset,
    space_id_t space_id,
    const char *space_name,
    ulint byte_offset,
    ulint len,
    IO_request io_request
  );

private:
  /** The number of fsyncs done to the log */
  ulint m_n_log_flushes{};

  /** Number of pending redo log flushes */
  ulint m_n_pending_log_flushes{};

  /** Number of pending tablespace flushes */
  ulint m_n_pending_tablespace_flushes{};

  /** When program is run, the default directory "." is the current datadir,
  but in ibbackup we must set it explicitly; the path must NOT contain the
  trailing '/' or '' */
  const char *m_path_to_client_datadir{};

  /** The mutex protecting the cache */
  mutable mutex_t m_mutex{};

  /** Map from space id to tablespace instance. */
  std::unordered_map<space_id_t, fil_space_t*> m_space_by_id{};

  /** Map from space name to the tablespace instance */
  std::unordered_map<std::string_view, fil_space_t*> m_space_by_name{};

  /** Number of files currently open */
  ulint m_n_open{};

  /** n_open is not allowed to exceed this */
  ulint m_max_n_open{};

  /** When we write to a file we increment this by one */
  int64_t m_modification_counter{};

  /** Maximum space id in the existing tables, or assigned during the time the
  server has been up; at an InnoDB startup we scan the data dictionary and set
  here the maximum of the space id's of the tables there */
  ulint m_max_assigned_id{};

  /** A counter which is incremented for every space object memory creation;
  every space mem object gets a 'timestamp' from this; in DISCARD/ IMPORT
  this is used to check if we should ignore an insert buffer merge request */
  int64_t m_tablespace_version{};

  /** Base node for the list of those tablespaces whose files contain unflushed
  writes; those spaces have at least one file node where m_modification_counter
  > m_flush_counter */
  UT_LIST_BASE_NODE_T(fil_space_t, m_unflushed_spaces) m_unflushed_spaces;

  /** List of all file spaces */
  UT_LIST_BASE_NODE_T(fil_space_t, m_space_list) m_space_list;
};

extern Fil *sys_fil;
