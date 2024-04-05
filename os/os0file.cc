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

/** @file os/os0file.c
The interface to the operating system file i/o primitives

Created 10/21/1995 Heikki Tuuri
*******************************************************/

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "api0misc.h"
#include "buf0buf.h"
#include "fil0fil.h"
#include "os0file.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "ut0mem.h"

#include "os0sync.h"
#include "os0thread.h"

/* This specifies the file permissions InnoDB uses when it creates files in
Unix; the value of os_innodb_umask is initialized in ha_innodb.cc to
my_umask */

/** Umask for creating files */
ulint os_innodb_umask = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;

#ifdef UNIV_DO_FLUSH
/* If the following is set to true, we do not call os_file_flush in every
os_file_write. We can set this true when the doublewrite buffer is used. */
bool os_do_not_call_flush_at_each_write = false;
#else
/* We do not call os_file_flush in every os_file_write. */
#endif /* UNIV_DO_FLUSH */

/* In simulated aio, merge at most this many consecutive i/os */
#define OS_AIO_MERGE_N_CONSECUTIVE 64

/** If this flag is true, then we will use the native aio of the
OS (provided we compiled Innobase with it in), otherwise we will
use simulated aio we build below with threads */

bool os_aio_use_native_aio = false;

/** Flag: enable debug printout for asynchronous i/o */
bool os_aio_print_debug = false;

/** The asynchronous i/o array slot structure */
typedef struct os_aio_slot_struct os_aio_slot_t;

/** The asynchronous i/o array slot structure */
struct os_aio_slot_struct {
  bool is_read;            /*!< true if a read operation */
  ulint pos;               /*!< index of the slot in the aio
                           array */
  bool reserved;           /*!< true if this slot is reserved */
  time_t reservation_time; /*!< time when reserved */
  ulint len;               /*!< length of the block to read or
                           write */
  byte *buf;               /*!< buffer used in i/o */
  ulint type;              /*!< OS_FILE_READ or OS_FILE_WRITE */
  ulint offset;            /*!< 32 low bits of file offset in
                           bytes */
  ulint offset_high;       /*!< 32 high bits of file offset */
  os_file_t file;          /*!< file where to read or write */
  const char *name;        /*!< file name or path */
  bool io_already_done;    /*!< used only in simulated aio:
                            true if the physical i/o already
                            made and only the slot message
                            needs to be passed to the caller
                            of os_aio_simulated_handle */
  fil_node_t *message1;    /*!< message which is given by the */
  void *message2;          /*!< the requester of an aio operation
                           and which can be used to identify
                           which pending aio operation was
                           completed */
};

/** The asynchronous i/o array structure */
struct os_aio_array_t {
  /** The mutex protecting the aio array */
  OS_mutex *mutex;

  /** The event which is set to the signaled state when there is space in
  the aio outside the ibuf segment */
  Cond_var* not_full;

  /** The event which is set to the signaled state when there are no
  pending i/os in this array */
  Cond_var* is_empty;

  /** Total number of slots in the aio array.  This must be divisible by n_threads. */
  ulint n_slots;

  /** Number of segments in the aio array of pending aio requests. A thread
   * can wait separately for any one of the segments. */
  ulint n_segments;

  /** Number of reserved slots in the aio array outside the ibuf segment */
  ulint n_reserved;

  /** Pointer to the slots in the array */
  os_aio_slot_t *slots;
};

/** Array of events used in simulated aio */
static Cond_var* *os_aio_segment_wait_events = nullptr;

/** The aio arrays for non-ibuf i/o and ibuf i/o, as well as sync aio. These
are nullptr when the module has not yet been initialized. @{ */
static os_aio_array_t *os_aio_read_array = nullptr;  /*!< Reads */
static os_aio_array_t *os_aio_write_array = nullptr; /*!< Writes */
static os_aio_array_t *os_aio_ibuf_array = nullptr;  /*!< Insert buffer */
static os_aio_array_t *os_aio_log_array = nullptr;   /*!< Redo log */
static os_aio_array_t *os_aio_sync_array = nullptr;  /*!< Synchronous I/O */
/* @} */

/** Number of asynchronous I/O segments.  Set by os_aio_init(). */
static ulint os_aio_n_segments = ULINT_UNDEFINED;

/** If the following is true, read i/o handler threads try to
wait until a batch of new read requests have been posted */
static bool os_aio_recommend_sleep_for_read_threads = false;

ulint os_n_file_reads = 0;
ulint os_bytes_read_since_printout = 0;
ulint os_n_file_writes = 0;
ulint os_n_fsyncs = 0;
ulint os_n_file_reads_old = 0;
ulint os_n_file_writes_old = 0;
ulint os_n_fsyncs_old = 0;
time_t os_last_printout;

bool os_has_said_disk_full = false;

/** Number of pending os_file_pread() operations */
std::atomic<ulint> os_file_n_pending_preads{};

/** Number of pending os_file_pwrite() operations */
std::atomic<ulint> os_file_n_pending_pwrites{};

/** Number of pending write operations */
std::atomic<ulint> os_n_pending_writes{};

/** Number of pending read operations */
std::atomic<ulint> os_n_pending_reads{};

/* Array of English strings describing the current state of an
i/o handler thread */

static const char *srv_io_thread_op_info[SRV_MAX_N_IO_THREADS];
static const char *srv_io_thread_function[SRV_MAX_N_IO_THREADS];

void os_file_var_init() {
  os_aio_segment_wait_events = nullptr;
  os_aio_read_array = nullptr;
  os_aio_write_array = nullptr;
  os_aio_ibuf_array = nullptr;
  os_aio_log_array = nullptr;
  os_aio_sync_array = nullptr;
  os_aio_n_segments = ULINT_UNDEFINED;
  os_aio_recommend_sleep_for_read_threads = false;
  os_n_file_reads = 0;
  os_bytes_read_since_printout = 0;
  os_n_file_writes = 0;
  os_n_fsyncs = 0;
  os_n_file_reads_old = 0;
  os_n_file_writes_old = 0;
  os_n_fsyncs_old = 0;
  os_last_printout = 0;
  os_has_said_disk_full = false;
  os_file_n_pending_preads = 0;
  os_file_n_pending_pwrites = 0;
  os_n_pending_writes = 0;
  os_n_pending_reads = 0;

#ifdef UNIV_DO_FLUSH
  os_do_not_call_flush_at_each_write = false;
#endif /* UNIV_DO_FLUSH */
}

/** Returns a pointer to the nth slot in the aio array.
@return	pointer to slot */
static os_aio_slot_t *os_aio_array_get_nth_slot(
  os_aio_array_t *array, /*!< in: aio array */
  ulint index
) /*!< in: index of the slot */
{
  ut_a(index < array->n_slots);

  return (array->slots) + index;
}

/** Frees an aio wait array. */
static void os_aio_array_free(os_aio_array_t *array) /*!< in, own: array to free */
{
  os_mutex_destroy(array->mutex);
  os_event_free(array->not_full);
  os_event_free(array->is_empty);

  ut_delete(array->slots);
  array->slots = nullptr;

  ut_delete(array);
}

/** Shutdown the IO sub-system and free all the memory. */

void os_aio_close(void) {
  if (os_aio_segment_wait_events != nullptr) {
    ut_delete(os_aio_segment_wait_events);
    os_aio_segment_wait_events = nullptr;
  }

  if (os_aio_read_array != nullptr) {
    os_aio_array_free(os_aio_read_array);
    os_aio_read_array = nullptr;
  }

  if (os_aio_write_array != nullptr) {
    os_aio_array_free(os_aio_write_array);
    os_aio_write_array = nullptr;
  }

  if (os_aio_ibuf_array != nullptr) {
    os_aio_array_free(os_aio_ibuf_array);
    os_aio_ibuf_array = nullptr;
  }

  if (os_aio_log_array != nullptr) {
    os_aio_array_free(os_aio_log_array);
    os_aio_log_array = nullptr;
  }

  if (os_aio_sync_array != nullptr) {
    os_aio_array_free(os_aio_sync_array);
    os_aio_sync_array = nullptr;
  }
}

ulint os_file_get_last_error(bool report_all_errors) {
  auto err = (ulint)errno;

  if (report_all_errors || (err != ENOSPC && err != EEXIST)) {

    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Operating system error number %lu"
      " in a file operation.\n",
      (ulong)err
    );

    if (err == ENOENT) {
      ib_logger(
        ib_stream,
        "The error means the system"
        " cannot find the path specified.\n"
      );

      if (srv_is_being_started) {
        ib_logger(
          ib_stream,
          "If you are installing InnoDB,"
          " remember that you must create\n"
          "directories yourself, InnoDB"
          " does not create them.\n"
        );
      }
    } else if (err == EACCES) {
      ib_logger(
        ib_stream,
        "The error means your application "
        "does not have the access rights to\n"
        "the directory.\n"
      );
    } else {
      if (strerror((int)err) != nullptr) {
        ib_logger(
          ib_stream,
          "Error number %lu"
          " means '%s'.\n",
          err,
          strerror((int)err)
        );
      }

      ib_logger(
        ib_stream,
        ""
        "Check InnoDB website for details\n"
      );
    }
  }

  if (err == ENOSPC) {
    return OS_FILE_DISK_FULL;
  } else if (err == ENOENT) {
    return OS_FILE_NOT_FOUND;
  } else if (err == EEXIST) {
    return OS_FILE_ALREADY_EXISTS;
  } else if (err == EXDEV || err == ENOTDIR || err == EISDIR) {
    return OS_FILE_PATH_ERROR;
  } else {
    return 100 + err;
  }
}

/** Does error handling when a file operation fails.
Conditionally exits (calling exit(3)) based on should_exit value and the
error type
@return	true if we should retry the operation */
static bool os_file_handle_error_cond_exit(
  const char *name,      /*!< in: name of a file or nullptr */
  const char *operation, /*!< in: operation */
  bool should_exit
) /*!< in: call exit(3) if unknown error
                            and this parameter is true */
{
  ulint err;

  err = os_file_get_last_error(false);

  if (err == OS_FILE_DISK_FULL) {
    /* We only print a warning about disk full once */

    if (os_has_said_disk_full) {

      return false;
    }

    if (name) {
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Encountered a problem with"
        " file %s\n",
        name
      );
    }

    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Disk is full. Try to clean the disk"
      " to free space.\n"
    );

    os_has_said_disk_full = true;

    return false;
  } else if (err == OS_FILE_AIO_RESOURCES_RESERVED) {

    return true;
  } else if (err == OS_FILE_ALREADY_EXISTS || err == OS_FILE_PATH_ERROR) {

    return false;
  } else if (err == OS_FILE_SHARING_VIOLATION) {

    os_thread_sleep(10000000); /* 10 sec */
    return true;
  } else if (err == OS_FILE_INSUFFICIENT_RESOURCE) {

    os_thread_sleep(100000); /* 100 ms */
    return true;
  } else if (err == OS_FILE_OPERATION_ABORTED) {

    os_thread_sleep(100000); /* 100 ms */
    return true;
  } else {
    if (name) {
      ib_logger(ib_stream, "File name %s\n", name);
    }

    ib_logger(ib_stream, "File operation call: '%s'.\n", operation);

    if (should_exit) {
      log_fatal("Cannot continue operation.");
    }
  }

  return false;
}

/** Does error handling when a file operation fails.
@return	true if we should retry the operation */
static bool os_file_handle_error(
  const char *name, /*!< in: name of a file or nullptr */
  const char *operation
) /*!< in: operation */
{
  /* exit in case of unknown error */
  return os_file_handle_error_cond_exit(name, operation, true);
}

/** Does error handling when a file operation fails.
@return	true if we should retry the operation */
static bool os_file_handle_error_no_exit(
  const char *name, /*!< in: name of a file or nullptr */
  const char *operation
) /*!< in: operation */
{
  /* don't exit in case of unknown error */
  return os_file_handle_error_cond_exit(name, operation, false);
}

#undef USE_FILE_LOCK
#define USE_FILE_LOCK

#ifdef USE_FILE_LOCK
/** Obtain an exclusive lock on a file.
@param[in] fd                   File descriptor.
@paraam[in] name                File name.
@return	0 on success */
static int os_file_lock(int fd, const char *name) {
  struct flock lk;

  lk.l_type = F_WRLCK;
  lk.l_whence = SEEK_SET;
  lk.l_start = lk.l_len = 0;
  if (fcntl(fd, F_SETLK, &lk) == -1) {
    ib_logger(ib_stream, "Unable to lock %s, error: %d\n", name, errno);

    if (errno == EAGAIN || errno == EACCES) {
      ib_logger(
        ib_stream,
        "Check that you do not already have"
        " another instance of your application is\n"
        "using the same InnoDB data"
        " or log files.\n"
      );
    }

    return -1;
  }

  return 0;
}
#endif /* USE_FILE_LOCK */

void os_io_init_simple() {
}

FILE *os_file_create_tmpfile() {
  FILE *file = nullptr;
  auto fd = ib_create_tempfile("ib");

  if (fd >= 0) {
    file = fdopen(fd, "w+b");
  }

  if (file == nullptr) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Error: unable to create temporary file;"
      " errno: %d\n",
      errno
    );
    if (fd >= 0) {
      close(fd);
    }
  }

  return file;
}

os_file_dir_t os_file_opendir(const char *dirname, bool error_is_fatal) {
  auto dir = opendir(dirname);

  if (dir == nullptr && error_is_fatal) {
    os_file_handle_error(dirname, "opendir");
  }

  return dir;
}

int os_file_closedir(os_file_dir_t dir) {
  int ret;

  ret = closedir(dir);

  if (ret) {
    os_file_handle_error_no_exit(nullptr, "closedir");
  }

  return ret;
}

int os_file_readdir_next_file(const char *dirname, os_file_dir_t dir, os_file_stat_t *info) {
  ulint len;
  struct dirent *ent;
  int ret;
  struct stat statinfo;
#ifdef HAVE_READDIR_R
  char dirent_buf[sizeof(struct dirent) + _POSIX_PATH_MAX + 100];
  /* In /mysys/my_lib.c, _POSIX_PATH_MAX + 1 is used as
  the max file name len; but in most standards, the
  length is NAME_MAX; we add 100 to be even safer */
#endif

next_file:

#ifdef HAVE_READDIR_R
  ret = readdir_r(dir, (struct dirent *)dirent_buf, &ent);

  if (ret != 0) {
    ib_logger(ib_stream, "cannot read directory %s, error %lu\n", dirname, (ulong)ret);

    return -1;
  }

  if (ent == nullptr) {
    /* End of directory */
    return 1;
  }

  ut_a(strlen(ent->d_name) < _POSIX_PATH_MAX + 100 - 1);
#else
  ent = readdir(dir);

  if (ent == nullptr) {

    return 1;
  }
#endif /* HAVE_READDIR_R */
  ut_a(strlen(ent->d_name) < OS_FILE_MAX_PATH);

  if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) {

    goto next_file;
  }

  strcpy(info->name, ent->d_name);

  len = strlen(dirname) + strlen(ent->d_name) + 10;

  auto full_path = static_cast<char *>(ut_new(len));

  ut_snprintf(full_path, len, "%s/%s", dirname, ent->d_name);

  ret = stat(full_path, &statinfo);

  if (ret) {

    if (errno == ENOENT) {
      /* readdir() returned a file that does not exist,
      it must have been deleted in the meantime. Do what
      would have happened if the file was deleted before
      readdir() - ignore and go to the next entry.
      If this is the last entry then info->name will still
      contain the name of the deleted file when this
      function returns, but this is not an issue since the
      caller shouldn't be looking at info when end of
      directory is returned. */

      ut_delete(full_path);

      goto next_file;
    }

    os_file_handle_error_no_exit(full_path, "stat");

    ut_delete(full_path);

    return -1;
  }

  info->size = (off_t)statinfo.st_size;

  if (S_ISDIR(statinfo.st_mode)) {
    info->type = DIRECTORY;
  } else if (S_ISLNK(statinfo.st_mode)) {
    info->type = SYM_LINK;
  } else if (S_ISREG(statinfo.st_mode)) {
    info->type = REGULAR_FILE;
  } else {
    info->type = OS_FILE_TYPE_UNKNOWN;
  }

  ut_delete(full_path);

  return 0;
}

bool os_file_create_directory(const char *pathname, bool fail_if_exists) {
  auto rcode = mkdir(pathname, 0770);

  if (!(rcode == 0 || (errno == EEXIST && !fail_if_exists))) {
    /* failure */
    os_file_handle_error(pathname, "mkdir");

    return false;
  }

  return true;
}

os_file_t os_file_create_simple(const char *name, ulint create_mode, ulint access_type, bool *success) {
  os_file_t file;
  int create_flag;
  bool retry;

try_again:
  ut_a(name);

  if (create_mode == OS_FILE_OPEN) {
    if (access_type == OS_FILE_READ_ONLY) {
      create_flag = O_RDONLY;
    } else {
      create_flag = O_RDWR;
    }
  } else if (create_mode == OS_FILE_CREATE) {
    create_flag = O_RDWR | O_CREAT | O_EXCL;
  } else if (create_mode == OS_FILE_CREATE_PATH) {
    /* create subdirs along the path if needed  */
    *success = os_file_create_subdirs_if_needed(name);
    if (!*success) {
      return -1;
    }
    create_flag = O_RDWR | O_CREAT | O_EXCL;
    create_mode = OS_FILE_CREATE;
  } else {
    create_flag = 0;
    ut_error;
  }

  if (create_mode == OS_FILE_CREATE) {
    file = open(name, create_flag, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  } else {
    file = open(name, create_flag);
  }

  if (file == -1) {
    *success = false;

    retry = os_file_handle_error(name, create_mode == OS_FILE_OPEN ? "open" : "create");
    if (retry) {
      goto try_again;
    }
#ifdef USE_FILE_LOCK
  } else if (access_type == OS_FILE_READ_WRITE && os_file_lock(file, name)) {
    *success = false;
    close(file);
    file = -1;
#endif
  } else {
    *success = true;
  }

  return file;
}

os_file_t os_file_create_simple_no_error_handling(const char *name, ulint create_mode, ulint access_type, bool *success) {
  os_file_t file;
  int create_flag;

  ut_a(name);

  if (create_mode == OS_FILE_OPEN) {
    if (access_type == OS_FILE_READ_ONLY) {
      create_flag = O_RDONLY;
    } else {
      create_flag = O_RDWR;
    }
  } else if (create_mode == OS_FILE_CREATE) {
    create_flag = O_RDWR | O_CREAT | O_EXCL;
  } else {
    create_flag = 0;
    ut_error;
  }

  if (create_mode == OS_FILE_CREATE) {
    file = open(name, create_flag, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
  } else {
    file = open(name, create_flag);
  }

  if (file == -1) {
    *success = false;
#ifdef USE_FILE_LOCK
  } else if (access_type == OS_FILE_READ_WRITE && os_file_lock(file, name)) {
    *success = false;
    close(file);
    file = -1;
#endif /* USE_FILE_LOCK */
  } else {
    *success = true;
  }

  return file;
}

void os_file_set_nocache(int fd, const char *file_name, const char *operation_name) {
#if defined(O_DIRECT)
  if (fcntl(fd, F_SETFL, O_DIRECT) == -1) {
    int errno_save;
    errno_save = (int)errno;
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Failed to set O_DIRECT "
      "on file %s: %s: %s, continuing anyway\n",
      file_name,
      operation_name,
      strerror(errno_save)
    );
    if (errno_save == EINVAL) {
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  O_DIRECT is known to result in "
        "'Invalid argument' on Linux on tmpfs."
      );
    }
  }
#endif /* O_DIRECT */
}

os_file_t os_file_create(const char *name, ulint create_mode, ulint purpose, ulint type, bool *success) {
  os_file_t file;
  int create_flag;
  bool retry;
  const char *mode_str = nullptr;

try_again:
  ut_a(name);

  if (create_mode == OS_FILE_OPEN || create_mode == OS_FILE_OPEN_RAW || create_mode == OS_FILE_OPEN_RETRY) {
    mode_str = "OPEN";
    create_flag = O_RDWR;
  } else if (create_mode == OS_FILE_CREATE) {
    mode_str = "CREATE";
    create_flag = O_RDWR | O_CREAT | O_EXCL;
  } else if (create_mode == OS_FILE_OVERWRITE) {
    mode_str = "OVERWRITE";
    create_flag = O_RDWR | O_CREAT | O_TRUNC;
  } else {
    create_flag = 0;
    ut_error;
  }

  ut_a(type == OS_LOG_FILE || type == OS_DATA_FILE);

  ut_a(purpose == OS_FILE_AIO || purpose == OS_FILE_NORMAL);

#ifdef O_SYNC
  /* We let O_SYNC only affect log files; note that we map O_DSYNC to
  O_SYNC because the datasync options seemed to corrupt files in 2001
  in both Linux and Solaris */
  if (type == OS_LOG_FILE && srv_unix_file_flush_method == SRV_UNIX_O_DSYNC) {
    create_flag = create_flag | O_SYNC;
  }
#endif /* O_SYNC */

  file = open(name, create_flag, os_innodb_umask);

  if (file == -1) {
    *success = false;

    /* When srv_file_per_table is on, file creation failure may not
    be critical to the whole instance. Do not crash the server in
    case of unknown errors. */
    if (srv_file_per_table) {
      retry = os_file_handle_error_no_exit(name, create_mode == OS_FILE_CREATE ? "create" : "open");
    } else {
      retry = os_file_handle_error(name, create_mode == OS_FILE_CREATE ? "create" : "open");
    }

    if (retry) {
      goto try_again;
    } else {
      return file /* -1 */;
    }
  }

  *success = true;

  /* We disable OS caching (O_DIRECT) only on data files */
  if (type != OS_LOG_FILE && srv_unix_file_flush_method == SRV_UNIX_O_DIRECT) {

    os_file_set_nocache(file, name, mode_str);
  }

  return file;
}

bool os_file_delete_if_exists(const char *name) {
  int ret;

  ret = unlink(name);

  if (ret != 0 && errno != ENOENT) {
    os_file_handle_error_no_exit(name, "delete");

    return false;
  }

  return true;
}

bool os_file_delete(const char *name) {
  int ret;

  ret = unlink(name);

  if (ret != 0) {
    os_file_handle_error_no_exit(name, "delete");

    return false;
  }

  return true;
}

bool os_file_rename(const char *oldpath, const char *newpath) {
  int ret;

  ret = rename(oldpath, newpath);

  if (ret != 0) {
    os_file_handle_error_no_exit(oldpath, "rename");

    return false;
  }

  return true;
}

bool os_file_close(os_file_t file) {
  int ret;

  ret = close(file);

  if (ret == -1) {
    os_file_handle_error(nullptr, "close");

    return false;
  }

  return true;
}

bool os_file_get_size(os_file_t file, ulint *size, ulint *size_high) {
  off_t offs;

  offs = lseek(file, 0, SEEK_END);

  if (offs == ((off_t)-1)) {

    return false;
  }

  if (sizeof(off_t) > 4) {
    *size = (ulint)(offs & 0xFFFFFFFFUL);
    *size_high = (ulint)((ib_u64_t)offs >> 32);
  } else {
    *size = (ulint)offs;
    *size_high = 0;
  }

  return true;
}

int64_t os_file_get_size_as_iblonglong(os_file_t file) {
  ulint size;
  ulint size_high;
  bool success;

  success = os_file_get_size(file, &size, &size_high);

  if (!success) {

    return -1;
  }

  return (((int64_t)size_high) << 32) + (int64_t)size;
}

bool os_file_set_size(const char *name, os_file_t file, ulint size, ulint size_high) {
  ut_a(size == (size & 0xFFFFFFFF));

  off_t current_size{};
  off_t desired_size = (off_t)size + (((off_t)size_high) << 32);

  /* Write up to 1 megabyte at a time. */
  auto buf_size = ut_min(64, (ulint)(desired_size / UNIV_PAGE_SIZE)) * UNIV_PAGE_SIZE;
  auto buf2 = static_cast<byte *>(ut_new(buf_size + UNIV_PAGE_SIZE));

  /* Align the buffer for possible raw i/o */
  auto buf = static_cast<byte *>(ut_align(buf2, UNIV_PAGE_SIZE));

  /* Write buffer full of zeros */
  memset(buf, 0, buf_size);

  if (desired_size >= (off_t)(100 * 1024 * 1024)) {
    ib_logger(ib_stream, "Progress in MB:");
  }

  bool ret;

  while (current_size < desired_size) {
    ulint n_bytes;

    if (desired_size - current_size < (off_t)buf_size) {
      n_bytes = (ulint)(desired_size - current_size);
    } else {
      n_bytes = buf_size;
    }

    ret = os_file_write(name, file, buf, (ulint)(current_size & 0xFFFFFFFF), (ulint)(current_size >> 32), n_bytes);

    if (!ret) {
      ut_delete(buf2);
      goto error_handling;
    }

    /* Print about progress for each 100 MB written */
    if ((off_t)(current_size + n_bytes) / (off_t)(100 * 1024 * 1024) != current_size / (off_t)(100 * 1024 * 1024)) {

      ib_logger(ib_stream, " %lu00", (ulong)((current_size + n_bytes) / (off_t)(100 * 1024 * 1024)));
    }

    current_size += n_bytes;
  }

  if (desired_size >= (off_t)(100 * 1024 * 1024)) {

    ib_logger(ib_stream, "\n");
  }

  ut_delete(buf2);

  ret = os_file_flush(file);

  if (ret) {
    return true;
  }

error_handling:
  return false;
}

/** Sync file contenst to the device.
@param[in] file                 File to sync.
@return -1 on failure. */
static int os_file_fsync(os_file_t file) {
  int ret;
  bool retry{};
  int failures = 0;

  do {
    ret = fsync(file);

    ++os_n_fsyncs;

    if (ret == -1 && errno == ENOLCK) {
      if (failures % 100 == 0) {

        ut_print_timestamp(ib_stream);
        ib_logger(ib_stream, "fsync(): No locks available; retrying\n");
      }

      os_thread_sleep(200000 /* 0.2 sec */);

      ++failures;

      retry = true;
    } else {
      retry = false;
    }
  } while (retry);

  return ret;
}

bool os_file_flush(os_file_t file) {
  int ret;

#if defined(HAVE_DARWIN_THREADS)
#ifndef F_FULLFSYNC
  /* The following definition is from the Mac OS X 10.3 <sys/fcntl.h> */
#define F_FULLFSYNC 51 /* fsync + ask the drive to flush to the media */
#elif F_FULLFSYNC != 51
#error "F_FULLFSYNC != 51: ABI incompatibility with Mac OS X 10.3"
#endif
  /* Apple has disabled fsync() for internal disk drives in OS X. That
  caused corruption for a user when he tested a power outage. Let us in
  OS X use a nonstandard flush method recommended by an Apple
  engineer. */

  if (!srv_have_fullfsync) {

    /* If we are not on an operating system that supports this,
    then fall back to a plain fsync. */
    ret = os_file_fsync(file);

  } else {

    ret = fcntl(file, F_FULLFSYNC, nullptr);

    if (ret) {
      /* If we are not on a file system that supports this,
      then fall back to a plain fsync. */
      ret = os_file_fsync(file);
    }
  }
#else
  ret = os_file_fsync(file);
#endif

  if (ret == 0) {
    return true;
  }

  /* Since Linux returns EINVAL if the 'file' is actually a raw device,
  we choose to ignore that error if we are using raw disks */

  if (srv_start_raw_disk_in_use && errno == EINVAL) {

    return true;
  }

  ut_print_timestamp(ib_stream);

  ib_logger(ib_stream, "  Error: the OS said file flush did not succeed\n");

  os_file_handle_error(nullptr, "flush");

  /* It is a fatal error if a file flush does not succeed, because then
  the database can get corrupt on disk */
  ut_error;

  return false;
}

/** Does a synchronous read operation in Posix.
@return	number of bytes read, -1 if error */
static ssize_t os_file_pread(
  os_file_t file, /*!< in: handle to a file */
  void *buf,      /*!< in: buffer where to read */
  ulint n,        /*!< in: number of bytes to read */
  ulint offset,   /*!< in: least significant 32 bits of
                                           file offset from where to read */
  ulint offset_high
) /*!< in: most significant 32
                                           bits of offset */
{
  off_t offs;
  ssize_t n_bytes;

  ut_a((offset & 0xFFFFFFFFUL) == offset);

  /* If off_t is > 4 bytes in size, then we assume we can pass a
  64-bit address */

  if (sizeof(off_t) > 4) {
    offs = (off_t)offset + (((ib_u64_t)offset_high) << 32);

  } else {
    offs = (off_t)offset;

    if (offset_high > 0) {
      ib_logger(ib_stream, "Error: file read at offset > 4 GB\n");
    }
  }

  os_n_file_reads++;

  os_file_n_pending_preads.fetch_add(1, std::memory_order_relaxed);

  os_n_pending_reads.fetch_add(1, std::memory_order_relaxed);

  n_bytes = pread(file, buf, (ssize_t)n, offs);

  os_file_n_pending_preads.fetch_sub(1, std::memory_order_relaxed);

  os_n_pending_reads.fetch_sub(1, std::memory_order_relaxed);

  return n_bytes;
}

/** Does a synchronous write operation in Posix.
@return	number of bytes written, -1 if error */
static ssize_t os_file_pwrite(
  os_file_t file,  /*!< in: handle to a file */
  const void *buf, /*!< in: buffer from where to write */
  ulint n,         /*!< in: number of bytes to write */
  ulint offset,    /*!< in: least significant 32 bits of file
                                  offset where to write */
  ulint offset_high
) /*!< in: most significant 32 bits of
                             offset */
{
  ssize_t ret;
  off_t offs;

  ut_a((offset & 0xFFFFFFFFUL) == offset);

  /* If off_t is > 4 bytes in size, then we assume we can pass a
  64-bit address */

  if (sizeof(off_t) > 4) {
    offs = (off_t)offset + (((ib_u64_t)offset_high) << 32);
  } else {
    offs = (off_t)offset;

    if (offset_high > 0) {
      ib_logger(
        ib_stream,
        "Error: file write"
        " at offset > 4 GB\n"
      );
    }
  }

  os_n_file_writes++;

  os_file_n_pending_pwrites.fetch_add(1, std::memory_order_relaxed)
	  ;
  os_n_pending_writes.fetch_add(1, std::memory_order_relaxed);

  ret = pwrite(file, buf, (ssize_t)n, offs);

  os_file_n_pending_pwrites.fetch_sub(1, std::memory_order_relaxed);

  os_n_pending_writes.fetch_sub(1, std::memory_order_relaxed);

#ifdef UNIV_DO_FLUSH
  if (srv_unix_file_flush_method != SRV_UNIX_LITTLESYNC &&
      srv_unix_file_flush_method != SRV_UNIX_NOSYNC &&
      !os_do_not_call_flush_at_each_write) {

    /* Always do fsync to reduce the probability that when
    the OS crashes, a database page is only partially
    physically written to disk. */

    ut_a(true == os_file_flush(file));
  }
#endif /* UNIV_DO_FLUSH */

  return ret;
}

bool os_file_read(os_file_t file, void *buf, ulint offset, ulint offset_high, ulint n) {
  bool retry;
  ssize_t ret;

  os_bytes_read_since_printout += n;

try_again:
  ret = os_file_pread(file, buf, n, offset, offset_high);

  if ((ulint)ret == n) {

    return true;
  }

  ib_logger(
    ib_stream,
    "Error: tried to read %lu bytes at offset %lu %lu.\n"
    "Was only able to read %ld.\n",
    (ulong)n,
    (ulong)offset_high,
    (ulong)offset,
    (long)ret
  );
  retry = os_file_handle_error(nullptr, "read");

  if (retry) {
    goto try_again;
  }

  ib_logger(
    ib_stream,
    "Fatal error: cannot read from file."
    " OS error number %lu.\n",
    (ulong)errno
  );

  ut_error;

  return false;
}

bool os_file_read_no_error_handling(os_file_t file, void *buf, ulint offset, ulint offset_high, ulint n) {
  bool retry;
  ssize_t ret;

  os_bytes_read_since_printout += n;

try_again:
  ret = os_file_pread(file, buf, n, offset, offset_high);

  if ((ulint)ret == n) {

    return true;
  }
  retry = os_file_handle_error_no_exit(nullptr, "read");

  if (retry) {
    goto try_again;
  }

  return false;
}

void os_file_read_string(FILE *file, char *str, ulint size) {
  size_t flen;

  if (size == 0) {
    return;
  }

  rewind(file);
  flen = fread(str, 1, size - 1, file);
  str[flen] = '\0';
}

bool os_file_write(const char *name, os_file_t file, const void *buf, ulint offset, ulint offset_high, ulint n) {
  auto ret = os_file_pwrite(file, buf, n, offset, offset_high);

  if ((ulint)ret == n) {
    return true;
  }

  if (!os_has_said_disk_full) {
    ut_print_timestamp(ib_stream);

    ib_logger(
      ib_stream,
      "  Error: Write to file %s failed"
      " at offset %lu %lu.\n"
      "%lu bytes should have been written,"
      " only %ld were written.\n"
      "Operating system error number %lu.\n"
      "Check that your OS and file system"
      " support files of this size.\n"
      "Check also that the disk is not full"
      " or a disk quota exceeded.\n",
      name,
      offset_high,
      offset,
      n,
      (long int)ret,
      (ulint)errno
    );
    if (strerror(errno) != nullptr) {
      ib_logger(ib_stream, "Error number %lu means '%s'.\n", (ulint)errno, strerror(errno));
    }

    ib_logger(
      ib_stream,
      ""
      "Check InnoDB website for details\n"
    );

    os_has_said_disk_full = true;
  }

  return false;
}

bool os_file_status(const char *path, bool *exists, os_file_type_t *type) {
  int ret;
  struct stat statinfo;

  ret = stat(path, &statinfo);
  if (ret && (errno == ENOENT || errno == ENOTDIR)) {
    /* file does not exist */
    *exists = false;
    return true;
  } else if (ret) {
    /* file exists, but stat call failed */

    os_file_handle_error_no_exit(path, "stat");

    return false;
  }

  if (S_ISDIR(statinfo.st_mode)) {
    *type = DIRECTORY;
  } else if (S_ISLNK(statinfo.st_mode)) {
    *type = SYM_LINK;
  } else if (S_ISREG(statinfo.st_mode)) {
    *type = REGULAR_FILE;
  } else {
    *type = OS_FILE_TYPE_UNKNOWN;
  }

  *exists = true;

  return true;
}

bool os_file_get_status(const char *path, os_file_stat_t *stat_info) {
  int ret;
  struct stat statinfo;

  ret = stat(path, &statinfo);

  if (ret && (errno == ENOENT || errno == ENOTDIR)) {
    /* file does not exist */

    return false;
  } else if (ret) {
    /* file exists, but stat call failed */

    os_file_handle_error_no_exit(path, "stat");

    return false;
  }

  if (S_ISDIR(statinfo.st_mode)) {
    stat_info->type = DIRECTORY;
  } else if (S_ISLNK(statinfo.st_mode)) {
    stat_info->type = SYM_LINK;
  } else if (S_ISREG(statinfo.st_mode)) {
    stat_info->type = REGULAR_FILE;
  } else {
    stat_info->type = OS_FILE_TYPE_UNKNOWN;
  }

  stat_info->ctime = statinfo.st_ctime;
  stat_info->atime = statinfo.st_atime;
  stat_info->mtime = statinfo.st_mtime;
  stat_info->size = statinfo.st_size;

  return true;
}

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

char *os_file_dirname(const char *path) {
  /* Find the offset of the last slash */
  const char *last_slash = strrchr(path, SRV_PATH_SEPARATOR);

  if (!last_slash) {
    /* No slash in the path, return "." */

    return mem_strdup(".");
  }

  /* Ok, there is a slash */

  if (last_slash == path) {
    /* last slash is the first char of the path */

    return mem_strdup("/");
  }

  /* Non-trivial directory component */

  return mem_strdupl(path, last_slash - path);
}

bool os_file_create_subdirs_if_needed(const char *path) {
  os_file_type_t type;
  bool success;
  bool subdir_exists;
  auto subdir = os_file_dirname(path);

  if (strlen(subdir) == 1 && (*subdir == SRV_PATH_SEPARATOR || *subdir == '.')) {
    /* subdir is root or cwd, nothing to do */
    mem_free(subdir);

    return true;
  }

  /* Test if subdir exists */
  success = os_file_status(subdir, &subdir_exists, &type);
  if (success && !subdir_exists) {
    /* subdir does not exist, create it */
    success = os_file_create_subdirs_if_needed(subdir);
    if (!success) {
      mem_free(subdir);

      return false;
    }
    success = os_file_create_directory(subdir, false);
  }

  mem_free(subdir);

  return success;
}

/** Creates an aio wait array.
@return	own: aio array */
static os_aio_array_t *os_aio_array_create(
  ulint n, /*!< in: maximum number of pending aio operations
                      allowed; n must be divisible by n_segments */
  ulint n_segments
) /*!< in: number of segments in the aio array */
{
  ut_a(n > 0);
  ut_a(n_segments > 0);

  auto array = static_cast<os_aio_array_t *>(ut_new(sizeof(os_aio_array_t)));

  array->mutex = os_mutex_create(nullptr);
  array->not_full = os_event_create(nullptr);
  array->is_empty = os_event_create(nullptr);

  os_event_set(array->is_empty);

  array->n_slots = n;
  array->n_segments = n_segments;
  array->n_reserved = 0;
  array->slots = static_cast<os_aio_slot_t *>(ut_new(n * sizeof(os_aio_slot_t)));

  for (ulint i = 0; i < n; i++) {
    auto slot = os_aio_array_get_nth_slot(array, i);

    slot->pos = i;
    slot->reserved = false;
  }

  return array;
}

void os_aio_init(ulint n_per_seg, ulint n_read_segs, ulint n_write_segs, ulint n_slots_sync) {
  ulint n_segments = 2 + n_read_segs + n_write_segs;

  ut_ad(n_segments >= 4);

  os_io_init_simple();

  for (ulint i = 0; i < n_segments; i++) {
    os_set_io_thread_op_info(i, "not started yet");
  }

  /* ib_logger(ib_stream, "Array n per seg %lu\n", n_per_seg); */

  os_aio_ibuf_array = os_aio_array_create(n_per_seg, 1);

  srv_io_thread_function[0] = "insert buffer thread";

  os_aio_log_array = os_aio_array_create(n_per_seg, 1);

  srv_io_thread_function[1] = "log thread";

  os_aio_read_array = os_aio_array_create(n_read_segs * n_per_seg, n_read_segs);
  for (ulint i = 2; i < 2 + n_read_segs; i++) {
    ut_a(i < SRV_MAX_N_IO_THREADS);
    srv_io_thread_function[i] = "read thread";
  }

  os_aio_write_array = os_aio_array_create(n_write_segs * n_per_seg, n_write_segs);
  for (ulint i = 2 + n_read_segs; i < n_segments; i++) {
    ut_a(i < SRV_MAX_N_IO_THREADS);
    srv_io_thread_function[i] = "write thread";
  }

  os_aio_sync_array = os_aio_array_create(n_slots_sync, 1);

  os_aio_n_segments = n_segments;

  os_aio_validate();

  os_aio_segment_wait_events = static_cast<Cond_var* *>(ut_new(n_segments * sizeof(void *)));

  for (ulint i = 0; i < n_segments; i++) {
    os_aio_segment_wait_events[i] = os_event_create(nullptr);
  }

  os_last_printout = time(nullptr);
}

void os_aio_wake_all_threads_at_shutdown() {
  /* This can happen if we have to abort during the startup phase. */
  if (os_aio_segment_wait_events == nullptr) {
    return;
  }

  /* This loop wakes up all simulated ai/o threads */

  for (ulint i = 0; i < os_aio_n_segments; i++) {

    os_event_set(os_aio_segment_wait_events[i]);
  }
}

void os_aio_wait_until_no_pending_writes() {
  os_event_wait(os_aio_write_array->is_empty);
}

/** Calculates segment number for a slot.
@return segment number (which is the number used by, for example,
i/o-handler threads) */
static ulint os_aio_get_segment_no_from_slot(
  os_aio_array_t *array, /*!< in: aio wait array */
  os_aio_slot_t *slot
) /*!< in: slot in this array */
{
  ulint segment;
  ulint seg_len;

  if (array == os_aio_ibuf_array) {
    segment = 0;

  } else if (array == os_aio_log_array) {
    segment = 1;

  } else if (array == os_aio_read_array) {
    seg_len = os_aio_read_array->n_slots / os_aio_read_array->n_segments;

    segment = 2 + slot->pos / seg_len;
  } else {
    ut_a(array == os_aio_write_array);
    seg_len = os_aio_write_array->n_slots / os_aio_write_array->n_segments;

    segment = os_aio_read_array->n_segments + 2 + slot->pos / seg_len;
  }

  return segment;
}

/** Calculates local segment number and aio array from global segment number.
@return	local segment number within the aio array */
static ulint os_aio_get_array_and_local_segment(
  os_aio_array_t **array, /*!< out: aio wait array */
  ulint global_segment
) /*!< in: global segment number */
{
  ulint segment;

  ut_a(global_segment < os_aio_n_segments);

  if (global_segment == 0) {
    *array = os_aio_ibuf_array;
    segment = 0;

  } else if (global_segment == 1) {
    *array = os_aio_log_array;
    segment = 0;

  } else if (global_segment < os_aio_read_array->n_segments + 2) {
    *array = os_aio_read_array;

    segment = global_segment - 2;
  } else {
    *array = os_aio_write_array;

    segment = global_segment - (os_aio_read_array->n_segments + 2);
  }

  return segment;
}

/** Requests for a slot in the aio array. If no slot is available, waits until
not_full-event becomes signaled.
@return	pointer to slot */
static os_aio_slot_t *os_aio_array_reserve_slot(
  ulint type,            /*!< in: OS_FILE_READ or OS_FILE_WRITE */
  os_aio_array_t *array, /*!< in: aio array */
  fil_node_t *message1,  /*!< in: message to be passed along with
                          the aio operation */
  void *message2,        /*!< in: message to be passed along with
                          the aio operation */
  os_file_t file,        /*!< in: file handle */
  const char *name,      /*!< in: name of the file or path as a
                           null-terminated string */
  void *buf,             /*!< in: buffer where to read or from which
                           to write */
  ulint offset,          /*!< in: least significant 32 bits of file
                           offset */
  ulint offset_high,     /*!< in: most significant 32 bits of
                      offset */
  ulint len
) /*!< in: length of the block to read or write */
{
  os_aio_slot_t *slot;
  ulint i;
  ulint slots_per_seg;
  ulint local_seg;

  /* No need of a mutex. Only reading constant fields */
  slots_per_seg = array->n_slots / array->n_segments;

  /* We attempt to keep adjacent blocks in the same local
  segment. This can help in merging IO requests when we are
  doing simulated AIO */
  local_seg = (offset >> (UNIV_PAGE_SIZE_SHIFT + 6)) % array->n_segments;

loop:
  os_mutex_enter(array->mutex);

  if (array->n_reserved == array->n_slots) {
    os_mutex_exit(array->mutex);

    if (!os_aio_use_native_aio) {
      /* If the handler threads are suspended, wake them
      so that we get more slots */

      os_aio_simulated_wake_handler_threads();
    }

    os_event_wait(array->not_full);

    goto loop;
  }

  /* First try to find a slot in the preferred local segment */
  for (i = local_seg * slots_per_seg; i < array->n_slots; i++) {
    slot = os_aio_array_get_nth_slot(array, i);

    if (slot->reserved == false) {
      goto found;
    }
  }

  /* Fall back to a full scan. We are guaranteed to find a slot */
  for (i = 0;; i++) {
    slot = os_aio_array_get_nth_slot(array, i);

    if (slot->reserved == false) {
      goto found;
    }
  }

found:
  ut_a(slot->reserved == false);
  array->n_reserved++;

  if (array->n_reserved == 1) {
    os_event_reset(array->is_empty);
  }

  if (array->n_reserved == array->n_slots) {
    os_event_reset(array->not_full);
  }

  slot->reserved = true;
  slot->reservation_time = time(nullptr);
  slot->message1 = message1;
  slot->message2 = message2;
  slot->file = file;
  slot->name = name;
  slot->len = len;
  slot->type = type;
  slot->buf = static_cast<byte *>(buf);
  slot->offset = offset;
  slot->offset_high = offset_high;
  slot->io_already_done = false;

  os_mutex_exit(array->mutex);

  return slot;
}

/** Frees a slot in the aio array. */
static void os_aio_array_free_slot(
  os_aio_array_t *array, /*!< in: aio array */
  os_aio_slot_t *slot
) /*!< in: pointer to slot */
{
  ut_ad(array);
  ut_ad(slot);

  os_mutex_enter(array->mutex);

  ut_ad(slot->reserved);

  slot->reserved = false;

  array->n_reserved--;

  if (array->n_reserved == array->n_slots - 1) {
    os_event_set(array->not_full);
  }

  if (array->n_reserved == 0) {
    os_event_set(array->is_empty);
  }

  os_mutex_exit(array->mutex);
}

/** Wakes up a simulated aio i/o-handler thread if it has something to do. */
static void os_aio_simulated_wake_handler_thread(ulint global_segment) /*!< in: the number of the segment in the aio
                          arrays */
{
  os_aio_array_t *array;
  os_aio_slot_t *slot;
  ulint segment;
  ulint n;
  ulint i;

  ut_ad(!os_aio_use_native_aio);

  segment = os_aio_get_array_and_local_segment(&array, global_segment);

  n = array->n_slots / array->n_segments;

  /* Look through n slots after the segment * n'th slot */

  os_mutex_enter(array->mutex);

  for (i = 0; i < n; i++) {
    slot = os_aio_array_get_nth_slot(array, i + segment * n);

    if (slot->reserved) {
      /* Found an i/o request */

      break;
    }
  }

  os_mutex_exit(array->mutex);

  if (i < n) {
    os_event_set(os_aio_segment_wait_events[global_segment]);
  }
}

/** Wakes up simulated aio i/o-handler threads if they have something to do. */

void os_aio_simulated_wake_handler_threads(void) {
  ulint i;

  if (os_aio_use_native_aio) {
    /* We do not use simulated aio: do nothing */

    return;
  }

  os_aio_recommend_sleep_for_read_threads = false;

  for (i = 0; i < os_aio_n_segments; i++) {
    os_aio_simulated_wake_handler_thread(i);
  }
}

void os_aio_simulated_put_read_threads_to_sleep() {}

bool os_aio(
  ulint type, ulint mode, const char *name, os_file_t file, void *buf, ulint offset, ulint offset_high, ulint n,
  fil_node_t *message1, void *message2
) {
  os_aio_array_t *array;
  os_aio_slot_t *slot;
  ulint err = 0;
  bool retry;
  ulint wake_later;

  ut_ad(file);
  ut_ad(buf);
  ut_ad(n > 0);
  ut_ad(n % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(offset % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(os_aio_validate());

  wake_later = mode & OS_AIO_SIMULATED_WAKE_LATER;
  mode = mode & (~OS_AIO_SIMULATED_WAKE_LATER);

  if (mode == OS_AIO_SYNC) {
    /* This is actually an ordinary synchronous read or write:
    no need to use an i/o-handler thread. NOTE that if we use
    Windows async i/o, Windows does not allow us to use
    ordinary synchronous os_file_read etc. on the same file,
    therefore we have built a special mechanism for synchronous
    wait in the Windows case. */

    if (type == OS_FILE_READ) {
      return os_file_read(file, buf, offset, offset_high, n);
    }

    ut_a(type == OS_FILE_WRITE);

    return os_file_write(name, file, buf, offset, offset_high, n);
  }

try_again:
  if (mode == OS_AIO_NORMAL) {
    if (type == OS_FILE_READ) {
      array = os_aio_read_array;
    } else {
      array = os_aio_write_array;
    }
  } else if (mode == OS_AIO_LOG) {

    array = os_aio_log_array;
  } else if (mode == OS_AIO_SYNC) {
    array = os_aio_sync_array;
  } else {
    array = nullptr; /* Eliminate compiler warning */
    ut_error;
  }

  slot = os_aio_array_reserve_slot(type, array, message1, message2, file, name, buf, offset, offset_high, n);
  if (type == OS_FILE_READ) {
    if (!os_aio_use_native_aio) {
      if (!wake_later) {
        os_aio_simulated_wake_handler_thread(os_aio_get_segment_no_from_slot(array, slot));
      }
    }
  } else if (type == OS_FILE_WRITE) {
    if (!os_aio_use_native_aio) {
      if (!wake_later) {
        os_aio_simulated_wake_handler_thread(os_aio_get_segment_no_from_slot(array, slot));
      }
    }
  } else {
    ut_error;
  }

  if (err == 0) {
    /* aio was queued successfully! */
    return true;
  }

  os_aio_array_free_slot(array, slot);

  retry = os_file_handle_error(name, type == OS_FILE_READ ? "aio read" : "aio write");
  if (retry) {

    goto try_again;
  }

  return false;
}

bool os_aio_simulated_handle(ulint global_segment, fil_node_t **message1, void **message2, ulint *type) {
  os_aio_array_t *array;
  ulint segment;
  os_aio_slot_t *slot;
  os_aio_slot_t *slot2;
  os_aio_slot_t *consecutive_ios[OS_AIO_MERGE_N_CONSECUTIVE];
  ulint n_consecutive;
  ulint total_len;
  ulint offs;
  ulint lowest_offset;
  ulint biggest_age;
  ulint age;
  byte *combined_buf;
  byte *combined_buf2;
  bool ret;
  ulint n;
  ulint i;

  /* Fix compiler warning */
  *consecutive_ios = nullptr;

  segment = os_aio_get_array_and_local_segment(&array, global_segment);

restart:
  /* NOTE! We only access constant fields in os_aio_array. Therefore
  we do not have to acquire the protecting mutex yet */

  os_set_io_thread_op_info(global_segment, "looking for i/o requests (a)");
  ut_ad(os_aio_validate());
  ut_ad(segment < array->n_segments);

  n = array->n_slots / array->n_segments;

  /* Look through n slots after the segment * n'th slot */

  if (array == os_aio_read_array && os_aio_recommend_sleep_for_read_threads) {

    /* Give other threads chance to add several i/os to the array
    at once. */

    goto recommended_sleep;
  }

  os_mutex_enter(array->mutex);

  os_set_io_thread_op_info(global_segment, "looking for i/o requests (b)");

  /* Check if there is a slot for which the i/o has already been done */

  for (i = 0; i < n; i++) {
    slot = os_aio_array_get_nth_slot(array, i + segment * n);

    if (slot->reserved && slot->io_already_done) {

      if (os_aio_print_debug) {
        ib_logger(
          ib_stream,
          "i/o for slot %lu"
          " already done, returning\n",
          (ulong)i
        );
      }

      ret = true;

      goto slot_io_done;
    }
  }

  n_consecutive = 0;

  /* If there are at least 2 seconds old requests, then pick the oldest
  one to prevent starvation. If several requests have the same age,
  then pick the one at the lowest offset. */

  biggest_age = 0;
  lowest_offset = ULINT_MAX;

  for (i = 0; i < n; i++) {
    slot = os_aio_array_get_nth_slot(array, i + segment * n);

    if (slot->reserved) {
      age = (ulint)difftime(time(nullptr), slot->reservation_time);

      if ((age >= 2 && age > biggest_age) || (age >= 2 && age == biggest_age && slot->offset < lowest_offset)) {

        /* Found an i/o request */
        consecutive_ios[0] = slot;

        n_consecutive = 1;

        biggest_age = age;
        lowest_offset = slot->offset;
      }
    }
  }

  if (n_consecutive == 0) {
    /* There were no old requests. Look for an i/o request at the
    lowest offset in the array (we ignore the high 32 bits of the
    offset in these heuristics) */

    lowest_offset = ULINT_MAX;

    for (i = 0; i < n; i++) {
      slot = os_aio_array_get_nth_slot(array, i + segment * n);

      if (slot->reserved && slot->offset < lowest_offset) {

        /* Found an i/o request */
        consecutive_ios[0] = slot;

        n_consecutive = 1;

        lowest_offset = slot->offset;
      }
    }
  }

  if (n_consecutive == 0) {

    /* No i/o requested at the moment */

    goto wait_for_io;
  }

  slot = consecutive_ios[0];

  /* Check if there are several consecutive blocks to read or write */

consecutive_loop:
  for (i = 0; i < n; i++) {
    slot2 = os_aio_array_get_nth_slot(array, i + segment * n);

    if (slot2->reserved && slot2 != slot &&
        slot2->offset == slot->offset + slot->len
        /* check that sum does not wrap over */
        && slot->offset + slot->len > slot->offset && slot2->offset_high == slot->offset_high && slot2->type == slot->type &&
        slot2->file == slot->file) {

      /* Found a consecutive i/o request */

      consecutive_ios[n_consecutive] = slot2;
      n_consecutive++;

      slot = slot2;

      if (n_consecutive < OS_AIO_MERGE_N_CONSECUTIVE) {

        goto consecutive_loop;
      } else {
        break;
      }
    }
  }

  os_set_io_thread_op_info(global_segment, "consecutive i/o requests");

  /* We have now collected n_consecutive i/o requests in the array;
  allocate a single buffer which can hold all data, and perform the
  i/o */

  total_len = 0;
  slot = consecutive_ios[0];

  for (i = 0; i < n_consecutive; i++) {
    total_len += consecutive_ios[i]->len;
  }

  if (n_consecutive == 1) {
    /* We can use the buffer of the i/o request */
    combined_buf = slot->buf;
    combined_buf2 = nullptr;
  } else {
    combined_buf2 = static_cast<byte *>(ut_new(total_len + UNIV_PAGE_SIZE));

    ut_a(combined_buf2);

    combined_buf = static_cast<byte *>(ut_align(combined_buf2, UNIV_PAGE_SIZE));
  }

  /* We release the array mutex for the time of the i/o: NOTE that
  this assumes that there is just one i/o-handler thread serving
  a single segment of slots! */

  os_mutex_exit(array->mutex);

  if (slot->type == OS_FILE_WRITE && n_consecutive > 1) {
    /* Copy the buffers to the combined buffer */
    offs = 0;

    for (i = 0; i < n_consecutive; i++) {

      memcpy(combined_buf + offs, consecutive_ios[i]->buf, consecutive_ios[i]->len);
      offs += consecutive_ios[i]->len;
    }
  }

  os_set_io_thread_op_info(global_segment, "doing file i/o");

  if (os_aio_print_debug) {
    ib_logger(
      ib_stream,
      "doing i/o of type %lu at offset %lu %lu,"
      " length %lu\n",
      (ulong)slot->type,
      (ulong)slot->offset_high,
      (ulong)slot->offset,
      (ulong)total_len
    );
  }

  /* Do the i/o with ordinary, synchronous i/o functions: */
  if (slot->type == OS_FILE_WRITE) {
    ret = os_file_write(slot->name, slot->file, combined_buf, slot->offset, slot->offset_high, total_len);
  } else {
    ret = os_file_read(slot->file, combined_buf, slot->offset, slot->offset_high, total_len);
  }

  ut_a(ret);
  os_set_io_thread_op_info(global_segment, "file i/o done");

  if (slot->type == OS_FILE_READ && n_consecutive > 1) {
    /* Copy the combined buffer to individual buffers */
    offs = 0;

    for (i = 0; i < n_consecutive; i++) {

      memcpy(consecutive_ios[i]->buf, combined_buf + offs, consecutive_ios[i]->len);
      offs += consecutive_ios[i]->len;
    }
  }

  if (combined_buf2) {
    ut_delete(combined_buf2);
  }

  os_mutex_enter(array->mutex);

  /* Mark the i/os done in slots */

  for (i = 0; i < n_consecutive; i++) {
    consecutive_ios[i]->io_already_done = true;
  }

  /* We return the messages for the first slot now, and if there were
  several slots, the messages will be returned with subsequent calls
  of this function */

slot_io_done:

  ut_a(slot->reserved);

  *message1 = slot->message1;
  *message2 = slot->message2;

  *type = slot->type;

  os_mutex_exit(array->mutex);

  os_aio_array_free_slot(array, slot);

  return ret;

wait_for_io:
  os_set_io_thread_op_info(global_segment, "resetting wait event");

  /* We wait here until there again can be i/os in the segment
  of this thread */

  os_event_reset(os_aio_segment_wait_events[global_segment]);

  os_mutex_exit(array->mutex);

recommended_sleep:
  os_set_io_thread_op_info(global_segment, "waiting for i/o request");

  os_event_wait(os_aio_segment_wait_events[global_segment]);

  if (os_aio_print_debug) {
    ib_logger(
      ib_stream,
      "i/o handler thread for i/o"
      " segment %lu wakes up\n",
      (ulong)global_segment
    );
  }

  goto restart;
}

/** Validates the consistency of an aio array.
@return	true if ok */
static bool os_aio_array_validate(os_aio_array_t *array) /*!< in: aio wait array */
{
  os_aio_slot_t *slot;
  ulint n_reserved = 0;
  ulint i;

  ut_a(array);

  os_mutex_enter(array->mutex);

  ut_a(array->n_slots > 0);
  ut_a(array->n_segments > 0);

  for (i = 0; i < array->n_slots; i++) {
    slot = os_aio_array_get_nth_slot(array, i);

    if (slot->reserved) {
      n_reserved++;
      ut_a(slot->len > 0);
    }
  }

  ut_a(array->n_reserved == n_reserved);

  os_mutex_exit(array->mutex);

  return true;
}

bool os_aio_validate(void) {
  os_aio_array_validate(os_aio_read_array);
  os_aio_array_validate(os_aio_write_array);
  os_aio_array_validate(os_aio_ibuf_array);
  os_aio_array_validate(os_aio_log_array);
  os_aio_array_validate(os_aio_sync_array);

  return true;
}

void os_aio_print(ib_stream_t ib_stream) {
  os_aio_array_t *array;
  os_aio_slot_t *slot;
  ulint n_reserved;
  time_t current_time;
  double time_elapsed;
  double avg_bytes_read;
  ulint i;

  for (i = 0; i < srv_n_file_io_threads; i++) {
    ib_logger(ib_stream, "I/O thread %lu state: %s (%s)", (ulong)i, srv_io_thread_op_info[i], srv_io_thread_function[i]);

    if (os_aio_segment_wait_events[i]->m_is_set) {
      ib_logger(ib_stream, " ev set");
    }

    ib_logger(ib_stream, "\n");
  }

  ib_logger(ib_stream, "Pending normal aio reads:");

  array = os_aio_read_array;
loop:
  ut_a(array);

  os_mutex_enter(array->mutex);

  ut_a(array->n_slots > 0);
  ut_a(array->n_segments > 0);

  n_reserved = 0;

  for (i = 0; i < array->n_slots; i++) {
    slot = os_aio_array_get_nth_slot(array, i);

    if (slot->reserved) {
      n_reserved++;
      ut_a(slot->len > 0);
    }
  }

  ut_a(array->n_reserved == n_reserved);

  ib_logger(ib_stream, " %lu", (ulong)n_reserved);

  os_mutex_exit(array->mutex);

  if (array == os_aio_read_array) {
    ib_logger(ib_stream, ", aio writes:");

    array = os_aio_write_array;

    goto loop;
  }

  if (array == os_aio_write_array) {
    ib_logger(ib_stream, ",\n ibuf aio reads:");
    array = os_aio_ibuf_array;

    goto loop;
  }

  if (array == os_aio_ibuf_array) {
    ib_logger(ib_stream, ", log i/o's:");
    array = os_aio_log_array;

    goto loop;
  }

  if (array == os_aio_log_array) {
    ib_logger(ib_stream, ", sync i/o's:");
    array = os_aio_sync_array;

    goto loop;
  }

  ib_logger(ib_stream, "\n");
  current_time = time(nullptr);
  time_elapsed = 0.001 + difftime(current_time, os_last_printout);

  ib_logger(
    ib_stream,
    "Pending flushes (fsync) log: %lu; buffer pool: %lu\n"
    "%lu OS file reads, %lu OS file writes, %lu OS fsyncs\n",
    (ulong)fil_n_pending_log_flushes,
    (ulong)fil_n_pending_tablespace_flushes,
    (ulong)os_n_file_reads,
    (ulong)os_n_file_writes,
    (ulong)os_n_fsyncs
  );

  if (os_file_n_pending_preads != 0 || os_file_n_pending_pwrites != 0) {
    ib_logger(
      ib_stream, "%lu pending preads, %lu pending pwrites\n",
      (ulong)os_file_n_pending_preads.load(),
      (ulong)os_file_n_pending_pwrites
    );
  }

  if (os_n_file_reads == os_n_file_reads_old) {
    avg_bytes_read = 0.0;
  } else {
    avg_bytes_read = (double)os_bytes_read_since_printout / (os_n_file_reads - os_n_file_reads_old);
  }

  ib_logger(
    ib_stream,
    "%.2f reads/s, %lu avg bytes/read,"
    " %.2f writes/s, %.2f fsyncs/s\n",
    (os_n_file_reads - os_n_file_reads_old) / time_elapsed,
    (ulong)avg_bytes_read,
    (os_n_file_writes - os_n_file_writes_old) / time_elapsed,
    (os_n_fsyncs - os_n_fsyncs_old) / time_elapsed
  );

  os_n_file_reads_old = os_n_file_reads;
  os_n_file_writes_old = os_n_file_writes;
  os_n_fsyncs_old = os_n_fsyncs;
  os_bytes_read_since_printout = 0;

  os_last_printout = current_time;
}

void os_aio_refresh_stats() {
  os_n_file_reads_old = os_n_file_reads;
  os_n_file_writes_old = os_n_file_writes;
  os_n_fsyncs_old = os_n_fsyncs;
  os_bytes_read_since_printout = 0;

  os_last_printout = time(nullptr);
}

#ifdef UNIV_DEBUG
bool os_aio_all_slots_free() {
  ulint n_res = 0;

  auto array = os_aio_read_array;

  os_mutex_enter(array->mutex);

  n_res += array->n_reserved;

  os_mutex_exit(array->mutex);

  array = os_aio_write_array;

  os_mutex_enter(array->mutex);

  n_res += array->n_reserved;

  os_mutex_exit(array->mutex);

  array = os_aio_ibuf_array;

  os_mutex_enter(array->mutex);

  n_res += array->n_reserved;

  os_mutex_exit(array->mutex);

  array = os_aio_log_array;

  os_mutex_enter(array->mutex);

  n_res += array->n_reserved;

  os_mutex_exit(array->mutex);

  array = os_aio_sync_array;

  os_mutex_enter(array->mutex);

  n_res += array->n_reserved;

  os_mutex_exit(array->mutex);

  if (n_res == 0) {

    return true;
  }

  return false;
}
#endif /* UNIV_DEBUG */

void os_set_io_thread_op_info(ulint i, const char *str) {
  ut_a(i < SRV_MAX_N_IO_THREADS);

  srv_io_thread_op_info[i] = str;
}
