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

#ifdef UNIV_DO_FLUSH
extern bool os_do_not_call_flush_at_each_write;
#endif /* UNIV_DO_FLUSH */
extern bool os_has_said_disk_full;
/** Flag: enable debug printout for asynchronous i/o */
extern bool os_aio_print_debug;

/** Number of pending os_file_pread() operations */
extern ulint os_file_n_pending_preads;
/** Number of pending os_file_pwrite() operations */
extern ulint os_file_n_pending_pwrites;

/** Number of pending read operations */
extern ulint os_n_pending_reads;
/** Number of pending write operations */
extern ulint os_n_pending_writes;

/** File handle */
typedef int os_file_t;
/** Convert a C file descriptor to a native file handle
@param fd	file descriptor
@return		native file handle */
#define OS_FILE_FROM_FD(fd) fd

/** Umask for creating files */
extern ulint os_innodb_umask;

/** If this flag is true, then we will use the native aio of the
OS (provided we compiled Innobase with it in), otherwise we will
use simulated aio we build below with threads */

extern bool os_aio_use_native_aio;

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
constexpr ulint OS_FILE_READ = 10;
constexpr ulint OS_FILE_WRITE = 11;

/** This can be ORed to type */
constexpr ulint OS_FILE_LOG = 256;
/* @} */

/*!< Win NT does not allow more than 64 */
constexpr ulint OS_AIO_N_PENDING_IOS_PER_THREAD = 32;

/** Modes for aio operations @{ */
constexpr ulint OS_AIO_NORMAL = 21;

/** Normal asynchronous i/o not for ibuf pages or ibuf bitmap pages */

/** Asynchronous i/o for the log */
constexpr ulint OS_AIO_LOG = 23;

/** Asynchronous i/o where the calling thread will itself wait for the i/o
to complete, doing also the job of the i/o-handler thread; can be used for
any pages, ibuf or non-ibuf.  This is used to save CPU time, as we can do
with fewer thread switches. Plain synchronous i/o is not as good, because
it must serialize the file seek and read or write, causing a bottleneck for
parallelism. */
constexpr ulint OS_AIO_SYNC = 24;

/** This can be ORed to mode in the call of os_aio(...), if the caller wants
to post several i/o requests in a batch, and only after that wake the
i/o-handler thread; this has effect only in simulated aio */
constexpr ulint OS_AIO_SIMULATED_WAKE_LATER = 512;

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

/* Maximum path string length in bytes when referring to tables with in the
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

/** Directory stream */
using os_file_dir_t = DIR *;

/** Gets the operating system version. */
ulint os_get_os_version();

/** Creates the seek mutexes used in positioned reads and writes. */
void os_io_init_simple();

/** Creates a temporary file.  This function is like tmpfile(3), but
the temporary file is created in the configured temporary directory.
On Netware, this function is like tmpfile(3), because the C run-time
library of Netware does not expose the delete-on-close flag.
@return	temporary file handle, or nullptr on error */
FILE *os_file_create_tmpfile();

/** The os_file_opendir() function opens a directory stream corresponding
to the directory named by the dirname argument. The directory stream is
positioned at the first entry. In both Unix and Windows we automatically
skip the '.' and '..' items at the start of the directory listing.
@param[in] dirname              Directory name; it must not contain
                                a trailing '\' or '/'
@param[in] err_is_fatal         true if we should treat an error as a
                                fatal error; if we try to open symlinks
				then we do not wish a fatal error if it
				happens not to be a directory
@return	directory stream, nullptr if error */
os_file_dir_t os_file_opendir(const char *dirname, bool error_is_fatal);

/** Closes a directory stream.
@param[in,out] dir              Directory anme.
@return	0 if success, -1 if failure */
int os_file_closedir(os_file_dir_t dir);

/** This function returns information of the next file in the directory. We jump
over the '.' and '..' entries in the directory.
@param[in] dirname              Directory name or path
@param[in,out] dir              Directory stream
@param[in,out] info             BUffer where the info is returned

@return	0 if ok, -1 if error, 1 if at the end of the directory */
int os_file_readdir_next_file(const char *dirname, os_file_dir_t dir, os_file_stat_t *info);

/** This function attempts to create a directory named pathname. The new
directory gets default permissions. On Unix, the permissions are (0770 &
~umask). If the directory exists already, nothing is done and the call succeeds,
unless the fail_if_exists arguments is true.
@return	true if call succeeds, false on error */
bool os_file_create_directory(
  const char *pathname, /*!< in: directory name as
                          null-terminated string */
  bool fail_if_exists
); /*!< in: if true, pre-existing directory
                           is treated as an error. */

/** A simple function to open or create a file.
@return own: handle to the file, not defined if error, error number
can be retrieved with os_file_get_last_error */
os_file_t os_file_create_simple(
  const char *name,  /*!< in: name of the file or path as a
                       null-terminated string */
  ulint create_mode, /*!< in: OS_FILE_OPEN if an existing file is
                   opened (if does not exist, error), or
                   OS_FILE_CREATE if a new file is created
                   (if exists, error), or
                   OS_FILE_CREATE_PATH if new file
                   (if exists, error) and subdirectories along
                   its path are created (if needed)*/
  ulint access_type, /*!< in: OS_FILE_READ_ONLY or
                   OS_FILE_READ_WRITE */
  bool *success
); /*!< out: true if succeed, false if error */

/** A simple function to open or create a file.
@return own: handle to the file, not defined if error, error number
can be retrieved with os_file_get_last_error */
os_file_t os_file_create_simple_no_error_handling(
  const char *name,  /*!< in: name of the file or path as a
                       null-terminated string */
  ulint create_mode, /*!< in: OS_FILE_OPEN if an existing file
                   is opened (if does not exist, error), or
                   OS_FILE_CREATE if a new file is created
                   (if exists, error) */
  ulint access_type, /*!< in: OS_FILE_READ_ONLY,
                   OS_FILE_READ_WRITE, or
                   OS_FILE_READ_ALLOW_DELETE; the last option is
                   used by a backup program reading the file */
  bool *success
); /*!< out: true if succeed, false if error */

/** Tries to disable OS caching on an opened file descriptor. */
void os_file_set_nocache(
  int fd,                /*!< in: file descriptor to alter */
  const char *file_name, /*!< in: file name, used in the
                                 diagnostic message */
  const char *operation_name
); /*!< in: "open" or "create"; used in the
                                 diagnostic message */

/** Opens an existing file or creates a new.
@return own: handle to the file, not defined if error, error number
can be retrieved with os_file_get_last_error */
os_file_t os_file_create(
  const char *name,  /*!< in: name of the file or path as a
                                  null-terminated string */
  ulint create_mode, /*!< in: OS_FILE_OPEN if an existing file
                              is opened (if does not exist, error), or
                              OS_FILE_CREATE if a new file is created
                              (if exists, error),
                              OS_FILE_OVERWRITE if a new file is created
                              or an old overwritten;
                              OS_FILE_OPEN_RAW, if a raw device or disk
                              partition should be opened */
  ulint purpose,     /*!< in: OS_FILE_AIO, if asynchronous,
                                  non-buffered i/o is desired,
                                  OS_FILE_NORMAL, if any normal file;
                                  NOTE that it also depends on type, os_aio_..
                                  and srv_.. variables whether we really use
                                  async i/o or unbuffered i/o: look in the
                                  function source code for the exact rules */
  ulint type,        /*!< in: OS_DATA_FILE or OS_LOG_FILE */
  bool *success
); /*!< out: true if succeed, false if error */

/** Deletes a file. The file has to be closed before calling this.
@return	true if success */
bool os_file_delete(const char *name); /*!< in: file path as a null-terminated string */

/** Deletes a file if it exists. The file has to be closed before calling this.
@return	true if success */
bool os_file_delete_if_exists(const char *name); /*!< in: file path as a null-terminated string */

/** Renames a file (can also move it to another directory). It is safest that
the file is closed before calling this function.
@return	true if success */
bool os_file_rename(
  const char *oldpath, /*!< in: old file path as a
                                           null-terminated string */
  const char *newpath
); /*!< in: new file path */

/** Closes a file handle. In case of error, error number can be retrieved with
os_file_get_last_error.
@return	true if success */
bool os_file_close(os_file_t file); /*!< in, own: handle to a file */

/** Gets a file size.
@return	true if success */
bool os_file_get_size(
  os_file_t file, /*!< in: handle to a file */
  ulint *size,    /*!< out: least significant 32 bits of file
                       size */
  ulint *size_high
); /*!< out: most significant 32 bits of size */

/** Gets file size as a 64-bit integer int64_t.
@return	size in bytes, -1 if error */
int64_t os_file_get_size_as_iblonglong(os_file_t file); /*!< in: handle to a file */

/** Write the specified number of zeros to a newly created file.
@return	true if success */
bool os_file_set_size(
  const char *name, /*!< in: name of the file or path as a
                      null-terminated string */
  os_file_t file,   /*!< in: handle to a file */
  ulint size,       /*!< in: least significant 32 bits of file
                      size */
  ulint size_high
); /*!< in: most significant 32 bits of size */

/** Truncates a file at its current position.
@return	true if success */
bool os_file_set_eof(FILE *file); /*!< in: file to be truncated */

/** Flushes the write buffers of a given file to the disk.
@return	true if success */
bool os_file_flush(os_file_t file); /*!< in, own: handle to a file */

/** Retrieves the last error number if an error occurs in a file io function.
The number should be retrieved before any other OS calls (because they may
overwrite the error number). If the number is not known to this program,
the OS error number + 100 is returned.
@return	error number, or OS error number + 100 */
ulint os_file_get_last_error(bool report_all_errors); /*!< in: true if we want an error message
                              printed of all errors */

/** Requests a synchronous read operation.
@return	true if request was successful, false if fail */
bool os_file_read(
  os_file_t file,    /*!< in: handle to a file */
  void *buf,         /*!< in: buffer where to read */
  ulint offset,      /*!< in: least significant 32 bits of file
                                     offset where to read */
  ulint offset_high, /*!< in: most significant 32 bits of
                                 offset */
  ulint n
); /*!< in: number of bytes to read */

/** Rewind file to its start, read at most size - 1 bytes from it to str, and
NUL-terminate str. All errors are silently ignored. This function is
mostly meant to be used with temporary files. */
void os_file_read_string(
  FILE *file, /*!< in: file to read from */
  char *str,  /*!< in: buffer where to read */
  ulint size
); /*!< in: size of buffer */

/** Requests a synchronous positioned read operation. This function does not do
any error handling. In case of error it returns false.
@return	true if request was successful, false if fail */
bool os_file_read_no_error_handling(
  os_file_t file,    /*!< in: handle to a file */
  void *buf,         /*!< in: buffer where to read */
  ulint offset,      /*!< in: least significant 32 bits of file
                       offset where to read */
  ulint offset_high, /*!< in: most significant 32 bits of
                   offset */
  ulint n
); /*!< in: number of bytes to read */

/** Requests a synchronous write operation.
@return	true if request was successful, false if fail */
bool os_file_write(
  const char *name,  /*!< in: name of the file or path as a
                                       null-terminated string */
  os_file_t file,    /*!< in: handle to a file */
  const void *buf,   /*!< in: buffer from which to write */
  ulint offset,      /*!< in: least significant 32 bits of file
                                      offset where to write */
  ulint offset_high, /*!< in: most significant 32 bits of
                                  offset */
  ulint n
); /*!< in: number of bytes to write */

/** Check the existence and type of the given file.
@return	true if call succeeded */
bool os_file_status(
  const char *path, /*!< in:	pathname of the file */
  bool *exists,     /*!< out: true if file exists */
  os_file_type_t *type
); /*!< out: type of the file (if it exists) */

/** The function os_file_dirname returns a directory component of a
null-terminated pathname string.  In the usual case, dirname returns
the string up to, but not including, the final '/', and basename
is the component following the final '/'.  Trailing '/' charac�
ters are not counted as part of the pathname.

If path does not contain a slash, dirname returns the string ".".

Concatenating the string returned by dirname, a "/", and the basename
yields a complete pathname.

The return value is  a copy of the directory component of the pathname.
The copy is allocated from heap. It is the caller responsibility
to free it after it is no longer needed.

The following list of examples (taken from SUSv2) shows the strings
returned by dirname and basename for different paths:

       path	      dirname	     basename
       "/usr/lib"     "/usr"	     "lib"
       "/usr/"	      "/"	     "usr"
       "usr"	      "."	     "usr"
       "/"	      "/"	     "/"
       "."	      "."	     "."
       ".."	      "."	     ".."

@return	own: directory component of the pathname */
char *os_file_dirname(const char *path); /*!< in: pathname */

/** Creates all missing subdirectories along the given path.
@return	true if call succeeded false otherwise */
bool os_file_create_subdirs_if_needed(const char *path); /*!< in: path name */

/** Initializes the asynchronous io system. Creates one array each for ibuf
and log i/o. Also creates one array each for read and write where each
array is divided logically into n_read_segs and n_write_segs
respectively. The caller must create an i/o handler thread for each
segment in these arrays. This function also creates the sync array.
No i/o handler thread needs to be created for that */

void os_aio_init(
  ulint n_per_seg,    /*<! in: maximum number of pending aio
                                      operations allowed per segment */
  ulint n_read_segs,  /*<! in: number of reader threads */
  ulint n_write_segs, /*<! in: number of writer threads */
  ulint n_slots_sync
); /*<! in: number of slots in the sync aio
                                      array */

/** Frees the asynchronous io system. */
void os_aio_free();

/** Requests an asynchronous i/o operation.
@return	true if request was queued successfully, false if fail */
bool os_aio(
  ulint type,           /*!< in: OS_FILE_READ or OS_FILE_WRITE */
  ulint mode,           /*!< in: OS_AIO_NORMAL, ..., possibly ORed
                                  to OS_AIO_SIMULATED_WAKE_LATER: the
                                  last flag advises this function not to wake
                                  i/o-handler threads, but the caller will
                                  do the waking explicitly later, in this
                                  way the caller can post several requests in
                                  a batch; NOTE that the batch must not be
                                  so big that it exhausts the slots in aio
                                  arrays! NOTE that a simulated batch
                                  may introduce hidden chances of deadlocks,
                                  because i/os are not actually handled until
                                  all have been posted: use with great
                                  caution! */
  const char *name,     /*!< in: name of the file or path as a
                                  null-terminated string */
  os_file_t file,       /*!< in: handle to a file */
  void *buf,            /*!< in: buffer where to read or from which
                                  to write */
  ulint offset,         /*!< in: least significant 32 bits of file
                                  offset where to read or write */
  ulint offset_high,    /*!< in: most significant 32 bits of
                             offset */
  ulint n,              /*!< in: number of bytes to read or write */
  fil_node_t *message1, /*!< in: message for the aio handler
                                 (can be used to identify a completed
                                 aio operation); ignored if mode is
                                 OS_AIO_SYNC */
  void *message2
); /*!< in: message for the aio handler
                                (can be used to identify a completed
                                aio operation); ignored if mode is
                                OS_AIO_SYNC */
/** Wakes up all async i/o threads so that they know to exit themselves in
shutdown. */
void os_aio_wake_all_threads_at_shutdown(void);

/** Waits until there are no pending writes in os_aio_write_array. There can
be other, synchronous, pending writes. */
void os_aio_wait_until_no_pending_writes(void);

/** Wakes up simulated aio i/o-handler threads if they have something to do. */
void os_aio_simulated_wake_handler_threads(void);

/** This function can be called if one wants to post a batch of reads and
prefers an i/o-handler thread to handle them all at once later. You must
call os_aio_simulated_wake_handler_threads later to ensure the threads
are not left sleeping! */
void os_aio_simulated_put_read_threads_to_sleep(void);

/** Does simulated aio. This function should be called by an i/o-handler
thread.
@return	true if the aio operation succeeded */
bool os_aio_simulated_handle(
  ulint segment,         /*!< in: the number of the segment in the aio
                                   arrays to wait for; segment 0 is the ibuf
                                   i/o thread, segment 1 the log i/o thread,
                                   then follow the non-ibuf read threads, and as
                                   the last are the non-ibuf write threads */
  fil_node_t **message1, /*!< out: the messages passed with the aio
                                   request; note that also in the case where
                                   the aio operation failed, these output
                                   parameters are valid and can be used to
                                   restart the operation, for example */
  void **message2, ulint *type
); /*!< out: OS_FILE_WRITE or ..._READ */

/** Validates the consistency of the aio system.
@return	true if ok */
bool os_aio_validate(void);

/** Prints info of the aio arrays. */
void os_aio_print(ib_stream_t ib_stream); /*!< in: stream where to print */

/** Refreshes the statistics used to print per-second averages. */
void os_aio_refresh_stats(void);

#ifdef UNIV_DEBUG
/** Checks that all slots in the system have been freed, that is, there are
no pending io operations. */
bool os_aio_all_slots_free(void);
#endif /* UNIV_DEBUG */

/** This function returns information about the specified file
@return	true if stat information found */
bool os_file_get_status(
  const char *path, /*!< in:	pathname of the file */
  os_file_stat_t *stat_info
); /*!< information of a file
                                                    in a directory */

/** Reset the variables. */
void os_file_var_init();

/** Close/Shutdown the IO sub-system and free all the memory. */
void os_aio_close();

/** Sets the info describing an i/o thread current state. */
void os_set_io_thread_op_info(
  ulint i, /*!< in: the 'segment' of the i/o thread */
  const char *str
); /*!< in: constant char string describing the
                      state */
