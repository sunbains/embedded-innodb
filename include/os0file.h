/**********************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

#include <filesystem>
#include <atomic>

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
bool os_file_rename(const char *oldpath, const char *newpath);

/**
 * @brief Closes a file handle. In case of error, error number can be retrieved with
 * os_file_get_last_error.
 *
 * @param file Handle to a file.
 * @return True if success.
 */
bool os_file_close(os_file_t file);

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
int64_t os_file_get_size_as_iblonglong(os_file_t file);

/**
 * @brief Write the specified number of zeros to a newly created file.
 *
 * @param name Name of the file or path as a null-terminated string.
 * @param file Handle to a file.
 * @param desired_size size in bytes.
 * @return True if success.
 */
bool os_file_set_size(const char *name, os_file_t file, off_t desired_size);

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
 * @brief Create an internal data structures.
 */
void os_file_init();

/**
 * @brief  Free any internal data structures.
 */
void os_file_free();

/**
 * @brief Prints info about the IO subsystem.
 *
 * @param ib_stream Stream where to print.
 */
void os_file_print(ib_stream_t ib_stream);

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

/** Get the ifile system statistics. */
void os_file_refresh_stats();

/** @return a text version of the path type. */
[[nodiscard]] inline const char *get_path_status(const std::filesystem::path &path) noexcept {
  namespace fs = std::filesystem;

  if (!fs::exists(path)) {
    return "does not exist";
  } else if (fs::is_regular_file(path)) {
    return "is a regular file";
  } else if (fs::is_directory(path)) {
    return "is a directory";
  } else if (fs::is_block_file(path)) {
    return "is a block device";
  } else if (fs::is_character_file(path)) {
    return "is a character device";
  } else if (fs::is_fifo(path)) {
    return "is a named IPC pipe";
  } else if (fs::is_socket(path)) {
    return "is a named IPC socket";
  } else if (fs::is_symlink(path)) {
    return "is a symlink";
  }

  ut_error;
  return "Unknown file status";
}

/**
 * @brief Get the permissions of a file in text representation.
 * 
 * @param[in] p File permissions.
 * 
 * @return Text representation of the file permissions.
 */
[[nodiscard]] inline std::string to_string(std::filesystem::perms p) noexcept {
    using std::filesystem::perms;
    std::ostringstream os{};

    auto print = [=](std::ostream& os, char op, perms perm) {
        os << (perms::none == (perm & p) ? '-' : op);
    };

    print(os, 'r', perms::owner_read);
    print(os, 'w', perms::owner_write);
    print(os, 'x', perms::owner_exec);
    print(os, 'r', perms::group_read);
    print(os, 'w', perms::group_write);
    print(os, 'x', perms::group_exec);
    print(os, 'r', perms::others_read);
    print(os, 'w', perms::others_write);
    print(os, 'x', perms::others_exec);

    return os.str();
}
