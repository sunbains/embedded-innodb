/**********************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2009, Percona Inc.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

This program is free software; you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
Public License for more details.

You should have received a copy of the GNU General Public License along
with this program; if not, write to the Free Software Foundation, Inc.,
59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

***********************************************************************/

/** @file include/os0file.h
The interface to the operating system file io

Created 10/21/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include <dirent.h>
#include <sys/stat.h>
#include <time.h>

/** File node of a tablespace or the log data space */
struct fil_node_t;

extern bool os_has_said_disk_full;

/** Number of pending os_file_pread() operations */
extern std::atomic<ulint> os_file_n_pending_preads;

/** Number of pending os_file_pwrite() operations */
extern std::atomic<ulint> os_file_n_pending_pwrites;

/** Number of pending read operations */
extern std::atomic<ulint> os_n_pending_reads;

/** Number of pending write operations */
extern std::atomic<ulint> os_n_pending_writes;

/** File handle */
typedef int os_file_t;

/** Convert a C file descriptor to a native file handle
@param fd	file descriptor
@return		native file handle */
#define OS_FILE_FROM_FD(fd) fd

/** The next value should be smaller or equal to the smallest sector size used
on any disk. A log block is required to be a portion of disk which is written
so that if the start and the end of a block get written to disk, then the
whole block gets written. This should be true even in most cases of a crash:
if this fails for a log block, then it is equivalent to a media failure in the
log. */

/** Options for file_create @{ */
constexpr ulint OS_FILE_OPEN = 51;
constexpr ulint OS_FILE_CREATE = 52;
constexpr ulint OS_FILE_OVERWRITE = 53;
constexpr ulint OS_FILE_OPEN_RAW = 54;
constexpr ulint OS_FILE_CREATE_PATH = 55;

/** For os_file_create() on the first ibdata file */
constexpr ulint OS_FILE_OPEN_RETRY = 56;

constexpr ulint OS_FILE_READ_ONLY = 333;
constexpr ulint OS_FILE_READ_WRITE = 444;

constexpr ulint OS_FILE_AIO = 61;
constexpr ulint OS_FILE_NORMAL = 62;

/* @} */

/** Types for file create @{ */
constexpr ulint OS_DATA_FILE = 100;
constexpr ulint OS_LOG_FILE = 101;
/* @} */

/** Error codes from os_file_get_last_error @{ */
constexpr ulint OS_FILE_NOT_FOUND = 71;
constexpr ulint OS_FILE_DISK_FULL = 72;
constexpr ulint OS_FILE_ALREADY_EXISTS = 73;
constexpr ulint OS_FILE_PATH_ERROR = 74;

/** Wait for OS aio resources to become available again */
constexpr ulint OS_FILE_AIO_RESOURCES_RESERVED = 75;

constexpr ulint OS_FILE_SHARING_VIOLATION = 76;
constexpr ulint OS_FILE_ERROR_NOT_SPECIFIED = 77;
constexpr ulint OS_FILE_INSUFFICIENT_RESOURCE = 78;
constexpr ulint OS_FILE_OPERATION_ABORTED = 79;

/* @} */

/** Types for aio operations @{ */
enum class IO_request {
  None,
  Async_read,
  Async_write,
  Async_log_read,
  Async_log_write,
  Sync_read,
  Sync_write,
  Sync_log_read,
  Sync_log_write,
};

/** This can be ORed to mode in the call of os_aio(...), if the caller wants
to post several i/o requests in a batch, and only after that wake the
i/o-handler thread; this has effect only in simulated aio */
constexpr ulint OS_AIO_SIMULATED_WAKE_LATER = 512;

/** Note: This value is from Windows NT, should be updated */
constexpr ulint OS_AIO_N_PENDING_IOS_PER_THREAD = 32;

/* @} */

extern ulint os_n_file_reads;
extern ulint os_n_file_writes;
extern ulint os_n_fsyncs;

/* File types for directory entry data type */

enum os_file_type_t {
  OS_FILE_TYPE_UNKNOWN = 0,

  REGULAR_FILE,

  DIRECTORY,

  SYM_LINK
};

/** Maximum path string length in bytes when referring to tables with in the
'./databasename/tablename.ibd' path format; we can allocate at least 2 buffers
of this size from the thread stack; that is why this should not be made much
bigger than 4000 bytes */
constexpr ulint OS_FILE_MAX_PATH = 4000;

/* Struct used in fetching information of a file in a directory */
struct os_file_stat_t {
  /** Path to a file */
  char name[OS_FILE_MAX_PATH];

  /** File type. */
  os_file_type_t type;

  /** File size in bytes. */
  off_t size;

  /** Creation time. */
  time_t ctime;

  /** Last modification time. */
  time_t mtime;

  /** Last access time. */
  time_t atime;
};

struct IO_ctx {
  void validate() const {
    ut_a(m_file != -1);
    ut_a(m_name != nullptr);
  }

  bool is_sync_request() const {
    switch (m_io_request) {
      case IO_request::Sync_read:
      case IO_request::Sync_write:
      case IO_request::Sync_log_read:
      case IO_request::Sync_log_write:
        return true;
      default:
        return false;
    }
  }

  bool is_log_request() const {
    switch (m_io_request) {
      case IO_request::Async_log_read:
      case IO_request::Async_log_write:
      case IO_request::Sync_log_read:
      case IO_request::Sync_log_write:
        return true;
      default:
        return false;
    }
  }

  bool is_read_request() const {
    switch (m_io_request) {
      case IO_request::Async_read:
      case IO_request::Async_log_read:
      case IO_request::Sync_read:
      case IO_request::Sync_log_read:
        return true;
      default:
        return false;
    }
  }

  /** File name. */
  const char *m_name{};

  /** File handle. */
  os_file_t m_file{-1};

  /** Request type. */
  IO_request m_io_request{};

  /**  Batch mode, we need to wake up the IO handler threads after posting the batch. */
  bool m_batch{};

  /** File meta data. */
  fil_node_t *m_fil_node{};

  /** User defined message. */
  void *m_msg{};
};

/** Directory stream */
using Dir = DIR *;

/** Gets the operating system version. */
ulint os_get_os_version();

/** Creates the seek mutexes used in positioned reads and writes. */
void os_io_init_simple();

/**
 * @brief Creates a temporary file. This function is like tmpfile(3), but
 * the temporary file is created in the configured temporary directory.
 * On Netware, this function is like tmpfile(3), because the C run-time
 * library of Netware does not expose the delete-on-close flag.
 *
 * @return temporary file handle, or nullptr on error
 */
FILE *os_file_create_tmpfile();

/**
 * @brief The os_file_opendir() function opens a directory stream corresponding
 * to the directory named by the dirname argument. The directory stream is
 * positioned at the first entry. In both Unix and Windows we automatically
 * skip the '.' and '..' items at the start of the directory listing.
 *
 * @param[in] dirname Directory name; it must not contain a trailing '\' or '/'
 * @param[in] err_is_fatal true if we should treat an error as a fatal error; if we try to open symlinks
 * then we do not wish a fatal error if it happens not to be a directory
 * @return directory stream, nullptr if error
 */
Dir os_file_opendir(const char *dirname, bool error_is_fatal);

/** Closes a directory stream.
@param[in,out] dir               Directory anme.
@return	0 if success, -1 if failure */
int os_file_closedir(Dir dir);

/** This function returns information of the next file in the directory. We jump
over the '.' and '..' entries in the directory.
@param[in] dirname              Directory name or path
@param[in,out] dir              Directory stream
@param[in,out] info             BUffer where the info is returned

@return	0 if ok, -1 if error, 1 if at the end of the directory */
int os_file_readdir_next_file(const char *dirname, Dir dir, os_file_stat_t *info);

/**
 * @brief This function attempts to create a directory named pathname.
 * 
 * The new directory gets default permissions. On Unix, the permissions
 * are (0770 & ~umask). If the directory exists already, nothing is done
 * and the call succeeds, unless the fail_if_exists arguments is true.
 * 
 * @param pathname        [in] Directory name as null-terminated string.
 * @param fail_if_exists  [in] If true, pre-existing directory is treated as an error.
 * @return true if call succeeds, false on error.
 */
bool os_file_create_directory(const char *pathname, bool fail_if_exists);

/**
 * @brief A simple function to open or create a file.
 *
 * @param name          [in] Name of the file or path as a null-terminated string.
 * @param create_mode   [in] OS_FILE_OPEN if an existing file is opened (if does 
 *                           ot exist, error), or OS_FILE_CREATE if a new file is
 *                           created (if exists, error), or OS_FILE_CREATE_PATH if
 *                           new file (if exists, error) and subdirectories along
 *                           its path are created (if needed).
 * @param access_type   [in] OS_FILE_READ_ONLY or OS_FILE_READ_WRITE.
 * @param success       [out] True if succeed, false if error.
 * @return os_file_t    Own handle to the file, not defined if error, error number can be
 *                      retrieved with os_file_get_last_error.
 */
os_file_t os_file_create_simple(const char *name, ulint create_mode, ulint access_type, bool *success); 

/**
 * @brief A simple function to open or create a file.
 *
 * @param name          [in] Name of the file or path as a null-terminated string.
 * @param create_mode   [in] OS_FILE_OPEN if an existing file is opened (if does not exist, error), or
 *                           OS_FILE_CREATE if a new file is created (if exists, error).
 * @param access_type   [in] OS_FILE_READ_ONLY, OS_FILE_READ_WRITE, or OS_FILE_READ_ALLOW_DELETE;
 *                           the last option is used by a backup program reading the file.
 * @param success       [out] True if succeed, false if error.
 * @return os_file_t    Own handle to the file, not defined if error, error number can be
 *                      retrieved with os_file_get_last_error.
 */
os_file_t os_file_create_simple_no_error_handling(
  const char *name,
  ulint create_mode,
  ulint access_type,
  bool *success
);

/**
 * @brief Tries to disable OS caching on an opened file descriptor.
 *
 * @param fd                [in] File descriptor to alter.
 * @param file_name         [in] File name, used in the diagnostic message.
 * @param operation_name    [in] "open" or "create"; used in the diagnostic message.
 */
void os_file_set_nocache(int fd, const char *file_name, const char *operation_name);

/**
 * @brief Opens an existing file or creates a new.
 *
 * @param name          [in] Name of the file or path as a null-terminated string.
 * @param create_mode   [in] OS_FILE_OPEN if an existing file is opened (if does not exist, error), or
 *                           OS_FILE_CREATE if a new file is created (if exists, error),
 *                           OS_FILE_OVERWRITE if a new file is created or an old overwritten;
 *                           OS_FILE_OPEN_RAW, if a raw device or disk partition should be opened.
 * @param purpose       [in] OS_FILE_AIO, if asynchronous, non-buffered i/o is desired,
 *                           OS_FILE_NORMAL, if any normal file;
 *                           NOTE that it also depends on type, os_aio_.. and srv_.. variables
 *                           whether we really use
 *                           async i/o or unbuffered i/o: look in the function source code for the exact rules.
 * @param type          [in] OS_DATA_FILE or OS_LOG_FILE.
 * @param success       [out] True if succeed, false if error.
 * @return os_file_t    Own handle to the file, not defined if error, error number can be
 *                      retrieved with os_file_get_last_error.
 */
os_file_t os_file_create(const char *name, ulint create_mode, ulint purpose, ulint type, bool *success);

/**
 * @brief Deletes a file. The file has to be closed before calling this.
 *
 * @param name File path as a null-terminated string.
 * @return True if success.
 */
bool os_file_delete(const char *name); /*!< in: file path as a null-terminated string */

/**
 * @brief Deletes a file if it exists. The file has to be closed before calling this.
 *
 * @param name File path as a null-terminated string.
 * @return True if success.
 */
bool os_file_delete_if_exists(const char *name); /*!< in: file path as a null-terminated string */

/**
 * @brief Renames a file (can also move it to another directory). It is safest that
 * the file is closed before calling this function.
 *
 * @param oldpath Old file path as a null-terminated string.
 * @param newpath New file path.
 * @return True if success.
 */
bool os_file_rename(
  const char *oldpath, /*!< in: old file path as a null-terminated string */
  const char *newpath
); /*!< in: new file path */

/**
 * @brief Closes a file handle. In case of error, error number can be retrieved with
 * os_file_get_last_error.
 *
 * @param file Handle to a file.
 * @return True if success.
 */
bool os_file_close(os_file_t file); /*!< in, own: handle to a file */

/**
 * @brief Gets a file size.
 *
 * @param file Handle to a file.
 * @param size Least significant 32 bits of file size.
 * @param size_high Most significant 32 bits of size.
 * @return True if success.
 */
bool os_file_get_size(os_file_t file, off_t *size);

/**
 * @brief Gets file size as a 64-bit integer int64_t.
 *
 * @param file Handle to a file.
 * @return Size in bytes, -1 if error.
 */
int64_t os_file_get_size_as_iblonglong(os_file_t file); /*!< in: handle to a file */

/**
 * @brief Write the specified number of zeros to a newly created file.
 *
 * @param name Name of the file or path as a null-terminated string.
 * @param file Handle to a file.
 * @param size Least significant 32 bits of file size.
 * @param size_high Most significant 32 bits of size.
 * @return True if success.
 */
bool os_file_set_size(const char *name, os_file_t file, ulint size, ulint size_high);

/**
 * @brief Truncates a file at its current position.
 *
 * @param file File to be truncated.
 * @return True if success.
 */
bool os_file_set_eof(FILE *file);

/**
 * @brief Flushes the write buffers of a given file to the disk.
 *
 * @param file Handle to a file.
 * @return True if success.
 */
bool os_file_flush(os_file_t file);

/**
 * @brief Retrieves the last error number if an error occurs in a file io function.
 * The number should be retrieved before any other OS calls (because they may
 * overwrite the error number). If the number is not known to this program,
 * the OS error number + 100 is returned.
 *
 * @param report_all_errors True if we want an error message printed of all errors.
 * @return Error number, or OS error number + 100.
 */
ulint os_file_get_last_error(bool report_all_errors);

/**
 * @brief Reads data from a file.
 *
 * @param file Handle to a file.
 * @param buf Buffer where to read.
 * @param n Number of bytes to read.
 * @param off byte offset in the file
 * @return true if the read operation was successful, false otherwise.
 */
bool os_file_read(os_file_t file, void *buf, ulint n, off_t off);

/**
 * @brief Reads at most size - 1 bytes from a file and NUL-terminates the string.
 *
 * @param file File to read from.
 * @param str Buffer where to read.
 * @param size Size of buffer.
 */
void os_file_read_string(FILE *file, char *str, ulint size);

/**
 * @brief Requests a synchronous positioned read operation.
 *
 * @param file Handle to a file.
 * @param buf Buffer where to read.
 * @param n Number of bytes to read.
 * @param off byte offset in the file
 * @return true if the request was successful, false if failed.
 */
bool os_file_read_no_error_handling(os_file_t file, void *buf, ulint n, off_t off);

/**
 * @brief Requests a synchronous write operation.
 *
 * @param name Name of the file or path as a null-terminated string.
 * @param file Handle to a file.
 * @param buf Buffer from which to write.
 * @param n Number of bytes to write.
 * @param off byte offset in the file
 * @return true if the request was successful, false if failed.
 */
bool os_file_write(const char *name, os_file_t file, const void *buf, ulint n, off_t off);

/**
 * @brief Checks the existence and type of the given file.
 *
 * @param path Pathname of the file.
 * @param exists True if file exists.
 * @param type Type of the file (if it exists).
 * @return true if the call succeeded, false otherwise.
 */
bool os_file_status(const char *path, bool *exists, os_file_type_t *type);

/** The function os_file_dirname returns a directory component of a
* null-terminated pathname string.  In the usual case, dirname returns
* the string up to, but not including, the final '/', and basename
* is the component following the final '/'.  Trailing '/' charac�
* ters are not counted as part of the pathname.
* 
* If path does not contain a slash, dirname returns the string ".".
* 
* Concatenating the string returned by dirname, a "/", and the basename
* yields a complete pathname.

* The return value is  a copy of the directory component of the pathname.
* The copy is allocated from heap. It is the caller responsibility
* to free it after it is no longer needed.
* 
* The following list of examples (taken from SUSv2) shows the strings
* returned by dirname and basename for different paths:
* 
*      path	      dirname	     basename
*      "/usr/lib"     "/usr"	     "lib"
*      "/usr/"	      "/"	     "usr"
*      "usr"	      "."	     "usr"
*      "/"	      "/"	     "/"
*      "."	      "."	     "."
*      ".."	      "."	     ".."
* @param[in] path	Pathname string
*
* @return	own: directory component of the pathname */
char *os_file_dirname(const char *path);

/**
 * @brief Creates all missing subdirectories along the given path.
 *
 * @param path Path name.
 * @return true if the call succeeded, false otherwise.
 */
bool os_file_create_subdirs_if_needed(const char *path);

/**
 * @brief Initializes the asynchronous io system.
 *
 * @param n_per_seg Maximum number of pending aio operations allowed per segment.
 * @param n_read_segs Number of reader threads.
 * @param n_write_segs Number of writer threads.
 * @param n_slots_sync Number of slots in the sync aio array.
 */
void os_aio_init(ulint n_per_seg, ulint n_read_segs, ulint n_write_segs, ulint n_slots_sync);

/**
 * @brief Frees the asynchronous io system.
 */
void os_aio_free();

/**
 * @brief Requests an asynchronous i/o operation.
 *
 * @param io_ctx Context of the i/o operation.
 * @param buf Buffer where to read or from which to write.
 * @param n Number of bytes to read or write.
 * @param offset Least significant 32 bits of file offset where to read or write.
 * @return true if the request was queued successfully, false if failed.
 */
bool os_aio(IO_ctx&& io_ctx, void *buf, ulint n, off_t off);

/**
 * @brief Wakes up all async i/o threads so that they know to exit themselves in shutdown.
 */
void os_aio_wake_all_threads_at_shutdown();

/**
 * @brief Waits until there are no pending writes in os_aio_write_array.
 *        There can be other, synchronous, pending writes.
 */
void os_aio_wait_until_no_pending_writes();

/**
 * @brief Wakes up simulated aio i/o-handler threads if they have something to do.
 */
void os_aio_simulated_wake_handler_threads();

/**
 * @brief Posts a batch of reads and prefers an i/o-handler thread to handle them all at once later.
 *        You must call os_aio_simulated_wake_handler_threads later to ensure the threads are not left sleeping!
 */
void os_aio_simulated_put_read_threads_to_sleep();

/**
 * @brief Does simulated aio. This function should be called by an i/o-handler thread.
 *
 * @param[in] segment The number of the segment in the aio arrays to wait for.
 * @param[out] io_ctx Context of the i/o operation.
 * @return true if the aio operation succeeded.
 */
bool os_aio_simulated_handle(ulint segment, IO_ctx &io_ctx);

/**
 * @brief Validates the consistency of the aio system.
 *
 * @return true if the aio system is consistent, false otherwise.
 */
bool os_aio_validate(void);

/**
 * @brief Prints info of the aio arrays.
 *
 * @param ib_stream Stream where to print.
 */
void os_aio_print(ib_stream_t ib_stream);

/**
 * @brief Refreshes the statistics used to print per-second averages.
 */
void os_aio_refresh_stats();

#ifdef UNIV_DEBUG
/**
 * @brief Checks that all slots in the system have been freed, that is, there are no pending io operations.
 *
 * @return true if all slots are free, false otherwise.
 */
bool os_aio_all_slots_free();
#endif /* UNIV_DEBUG */

/**
 * @brief Returns information about the specified file.
 *
 * @param path Pathname of the file.
 * @param stat_info Information of a file in a directory.
 * @return true if stat information found, false otherwise.
 */
bool os_file_get_status(const char *path, os_file_stat_t *stat_info);

/**
 * @brief Resets the variables.
 */
void os_file_var_init();

/**
 * @brief Closes/shuts down the IO sub-system and frees all the memory.
 */
void os_aio_close();

/**
 * @brief Sets the info describing an i/o thread current state.
 *
 * @param i The 'segment' of the i/o thread.
 * @param str Constant char string describing the state.
 */
void os_set_io_thread_op_info(ulint i, const char *str);

inline std::string to_string(IO_request request) {
  std::ostringstream os;

  switch (request) {
    case IO_request::None:
      os << "None";
      break;
    case IO_request::Async_read:
      os << "Async_read";
      break;
    case IO_request::Async_write:
      os << "Async_write";
      break;
    case IO_request::Async_log_read:
      os << "Async_log_read";
      break;
    case IO_request::Async_log_write:
      os << "Async_log_write";
      break;
    case IO_request::Sync_read:
      os << "Sync_read";
      break;
    case IO_request::Sync_write:
      os << "Sync_write";
      break;
    case IO_request::Sync_log_read:
      os << "Sync_log_read";
      break;
    case IO_request::Sync_log_write:
      os << "Sync_log_write";
      break;
  }

  return os.str();
}
