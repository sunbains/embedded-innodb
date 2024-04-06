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

#include <array>
#include <vector>

#include "api0misc.h"
#include "buf0buf.h"
#include "fil0fil.h"
#include "os0file.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "ut0mem.h"
#include "os0sync.h"
#include "os0thread.h"

/** Umask for creating files */
constexpr lint CREATE_MASK = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;

/* In simulated aio, merge at most this many consecutive i/os */
constexpr ulint OS_AIO_MERGE_N_CONSECUTIVE = 64;

/** If this flag is true, then we will use the native aio of the
OS (provided we compiled Innobase with it in), otherwise we will
use simulated aio we build below with threads */

/** The asynchronous i/o array slot structure */
struct AIO_slot {
  /** Time when reserved */
  time_t m_reservation_time{};

  /** Index of the slot in the aio array */
  uint32_t m_pos{};

  /** Length of the block to read or write */
  uint32_t m_len{};

  /** Buffer used in i/o */
  byte *m_buf{};

  /** File offset in bytes */
  off_t m_off{};

  /** Used only in simulated aio: true if the physical i/o already
   * made and only the slot message needs to be passed to the caller
   * of os_aio_simulated_handle */
  bool m_io_already_done{};
  
  /** true if this slot is reserved */
  bool m_reserved{};

  /** The IO context */
  IO_ctx m_io_ctx;
};

/** The asynchronous i/o array structure */
struct AIO_array {
  /** Constructor
   * @param[in] n_segments Number of segments in the aio array
   * @param[in] n_slots Number of slots in the aio array
   */
 explicit AIO_array(size_t n_segments, size_t n_slots);

  /* Destructor */
  ~AIO_array() noexcept;

  /* Mutex protecting the fields below. */
  OS_mutex *m_mutex{};

  /** The event which is set to the signaled state when there is space in
  the aio segment */
  Cond_var* m_not_full{};

  /** The event which is set to the signaled state when there are no
  pending i/os in this array */
  Cond_var* m_is_empty{};

  /** Number of segments in the aio array of pending aio requests. A thread
   * can wait separately for any one of the segments. */
  ulint m_n_segments{};

  /** Number of  Treserved slots in the aio array segment */
  ulint m_n_reserved{};

  /** Pointer to the slots in the array */
  std::vector<AIO_slot> m_slots{};
};

struct AIO_segment {
  ulint get_slot_count() {
    return m_array->m_slots.capacity() / m_array->m_n_segments;
  }

  AIO_slot *find_slot() {
    return nullptr;
  }

  void lock() {
    os_mutex_enter(m_array->m_mutex);
  }

  void unlock() {
    os_mutex_exit(m_array->m_mutex);
  }

  /**
   * Reserve a slot from the array.
   * @param[in] io_ctx IO context.
   * @param[in] ptr Pointer to the buffer for IO
   * @param[in] len Length of the buffer (to read/write)
   * @param[in] off Offset in the file.
   * @return an IO slot from the array. */
  AIO_slot *reserve_slot(const IO_ctx &io_ctx, void *ptr, ulint len, off_t off);

  /** Free the slot.
   * @param[in] slot Slot to free.
  */
  void free_slot(AIO_slot* slot);

  /** Create an instnce without the local segment number from the
  * IO mode and request type. *
  * @param[in] io_ctx IO context.
  * 
  * @return an AIO_segment instance. */
  static AIO_segment get_segment(const IO_ctx &io_ctx);

  /**
  * Calculates local segment number and aio array from global segment number.
  *
  * @param global_segment    in: global segment number
  *
  * @return the AIO_segment with the local segment number and the aio array.
  */
  static AIO_segment get_local_segment(ulint global_segment);
  
  /** Get a local segment number from the AIO array an slot ordinal value.
   * @param[in] slot AIO slot.
   * 
   * @return the local segment number.
  */
  ulint get_local_segment_no(const AIO_slot *slot) const;

  AIO_array *m_array{};
  ulint m_no{std::numeric_limits<ulint>::max()};
};

/** Array of events used in simulated aio */
static Cond_var* *os_aio_segment_wait_events = nullptr;

/** The aio arrays for i/o, as well as sync aio. These
are nullptr when the module has not yet been initialized. @{ */
static AIO_array *os_aio_log_array = nullptr;
static AIO_array *os_aio_read_array = nullptr;
static AIO_array *os_aio_write_array = nullptr;
static AIO_array *os_aio_sync_array = nullptr;
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
}

AIO_array::AIO_array(size_t n_segments, size_t n) {
  m_mutex = os_mutex_create(nullptr);
  m_not_full = os_event_create(nullptr);
  m_is_empty = os_event_create(nullptr);

  os_event_set(m_is_empty);

  m_n_reserved = 0;
  m_n_segments = n_segments;

  m_slots.resize(n);

  ulint i{};
  for (auto &slot : m_slots) {
    slot.m_pos = i++;
  }
}

AIO_array::~AIO_array() {
  os_mutex_destroy(m_mutex);
  os_event_free(m_not_full);
  os_event_free(m_is_empty);
}

/**
 * Frees an aio wait array.
 *
 * @param array - array to free
 */
static void os_aio_array_free(AIO_array *array) {
  call_destructor(array);
  ut_delete(array);
}

void os_aio_close() {
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
  if (report_all_errors || (errno != ENOSPC && errno != EEXIST)) {
    log_err("Operating system error number ", errno, " in a file operation.");

    if (errno == ENOENT) {
      log_err("The error means the system cannot find the path specified.");

      if (srv_is_being_started) {
        log_err(
          "If you are installing InnoDB, remember that you must create"
          " directories yourself, InnoDB does not create them.");
      }

    } else if (errno == EACCES) {

      log_err(
        "The error means your application does not have the access rights to"
        " the directory.");

    } else {

      log_err("errno maps to: ", strerror(errno));
    }
  }

  if (errno == ENOSPC) {
    return OS_FILE_DISK_FULL;
  } else if (errno == ENOENT) {
    return OS_FILE_NOT_FOUND;
  } else if (errno == EEXIST) {
    return OS_FILE_ALREADY_EXISTS;
  } else if (errno == EXDEV || errno == ENOTDIR || errno == EISDIR) {
    return OS_FILE_PATH_ERROR;
  } else {
    return 100 + errno;
  }
}

/**
 * Does error handling when a file operation fails.
 * Conditionally exits (calling exit(3)) based on should_exit value and the
 * error type
 *
 * @param[in] name      Name of a file or nullptr.
 * @param[in] operation Operation.
 * @param[in] should_exit Call exit(3) if unknown error and this parameter is true.
 * @return              True if we should retry the operation.
 */
static bool os_file_handle_error_cond_exit(const char *name, const char *operation, bool should_exit) {
  auto err = os_file_get_last_error(false);

  if (err == OS_FILE_DISK_FULL) {
    /* We only print a warning about disk full once */

    if (os_has_said_disk_full) {

      return false;
    }

    if (name != nullptr) {
      log_err("Encountered a problem with file ", name);
    }

    log_err("Disk is full. Try to clean the disk to free space.");

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

    if (name != nullptr) {
      log_warn("File name ", name);
    }

    log_warn("File operation call: '", operation, "'");

    if (should_exit) {
      log_fatal("Cannot continue operation.");
    }
  }

  return false;
}

/**
 * Does error handling when a file operation fails.
 * @param[in] name      Name of a file or nullptr.
 * @param[in] operation Operation.
 * @return              True if we should retry the operation.
 */
static bool os_file_handle_error(const char *name, const char *operation) {
  /* Exit in case of unknown error */
  return os_file_handle_error_cond_exit(name, operation, true);
}

/**
 * Does error handling when a file operation fails.
 *
 * @param[in] name      Name of a file or nullptr.
 * @param[in] operation Operation.
 * @return              True if we should retry the operation.
 */
static bool os_file_handle_error_no_exit(const char *name, const char *operation) {
  /* Don't exit in case of unknown error */
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
        "Check that you do not already have another instance of your application is"
        " using the same InnoDB data or log files.\n"
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
      "Unable to create temporary file; errno: %d\n", errno
    );
    if (fd >= 0) {
      close(fd);
    }
  }

  return file;
}

Dir os_file_opendir(const char *dirname, bool error_is_fatal) {
  auto dir = opendir(dirname);

  if (dir == nullptr && error_is_fatal) {
    os_file_handle_error(dirname, "opendir");
  }

  return dir;
}

int os_file_closedir(Dir dir) {
  int ret;

  ret = closedir(dir);

  if (ret) {
    os_file_handle_error_no_exit(nullptr, "closedir");
  }

  return ret;
}

int os_file_readdir_next_file(const char *dirname, Dir dir, os_file_stat_t *info) {
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
  ut_a(name != nullptr);

  for (;;) {
    int create_flag;
    os_file_t file{-1};

    if (create_mode == OS_FILE_OPEN) {
      if (access_type == OS_FILE_READ_ONLY) {
        create_flag = O_RDONLY;
      } else {
        create_flag = O_RDWR;
      }
    } else if (create_mode == OS_FILE_CREATE) {
      create_flag = O_RDWR | O_CREAT | O_EXCL;
    } else if (create_mode == OS_FILE_CREATE_PATH) {
      /* Create subdirs along the path if needed  */
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

      if (os_file_handle_error(name, create_mode == OS_FILE_OPEN ? "open" : "create")) {
        continue;
      }
    } else {
      *success = true;
      return file;
    }

    break;
  }

  return -1;
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
  if (fcntl(fd, F_SETFL, O_DIRECT) == -1) {
    auto errno_save = errno;

    ib_logger(ib_stream, "  Failed to set O_DIRECT on file %s: %s: %s, continuing anyway\n",
              file_name, operation_name, strerror(errno_save)
    );
    if (errno_save == EINVAL) {
      ib_logger(ib_stream, "  O_DIRECT is known to result in " "'Invalid argument' on Linux on tmpfs.");
    }
  }
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

  file = open(name, create_flag, CREATE_MASK);

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
  int ret = unlink(name);

  if (ret != 0 && errno != ENOENT) {
    os_file_handle_error_no_exit(name, "delete");

    return false;
  }

  return true;
}

bool os_file_delete(const char *name) {
  int ret = unlink(name);

  if (ret != 0) {
    os_file_handle_error_no_exit(name, "delete");

    return false;
  }

  return true;
}

bool os_file_rename(const char *oldpath, const char *newpath) {
  int ret = rename(oldpath, newpath);

  if (ret != 0) {
    os_file_handle_error_no_exit(oldpath, "rename");

    return false;
  }

  return true;
}

bool os_file_close(os_file_t file) {
  int ret = close(file);

  if (ret == -1) {
    os_file_handle_error(nullptr, "close");

    return false;
  }

  return true;
}

bool os_file_get_size(os_file_t file, off_t *off) {
  static_assert(sizeof(off_t) == 8, "sizeof(off_t) != 8");

  *off = lseek(file, 0, SEEK_END);

  return *off != ((off_t)-1);
}

int64_t os_file_get_size_as_iblonglong(os_file_t file) {
  off_t off;

  if (os_file_get_size(file, &off)) {
    return off;
  } else {
    return -1;
  }
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

    ret = os_file_write(name, file, buf, n_bytes, current_size);

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
  int ret = os_file_fsync(file);

  if (ret == 0) {
    return true;
  }

  /* Since Linux returns EINVAL if the 'file' is actually a raw device,
  we choose to ignore that error if we are using raw disks */

  if (srv_start_raw_disk_in_use && errno == EINVAL) {

    return true;
  }

  os_file_handle_error(nullptr, "flush");

  /* It is a fatal error if a file flush does not succeed, because then
  the database can get corrupt on disk */
  log_fatal("The OS said file flush did not succeed");

  return false;
}

/**
 * Does a synchronous read operation in Posix.
 *
 * @param[in] file Handle to a file.
 * @param[in] buf Buffer where to read.
 * @param[in] n Number of bytes to read.
 * @param[in] offset Least significant 32 bits of file offset from where to read.
 * @param[in] offset_high Most significant 32 bits of offset.
 *
 * @return Number of bytes read, -1 if error.
 */
static ssize_t os_file_pread(os_file_t file, void *buf, ulint n, off_t off) {
  /* If off_t is > 4 bytes in size, then we assume we can pass a
  64-bit address */

  ++os_n_file_reads;

  os_file_n_pending_preads.fetch_add(1, std::memory_order_relaxed);

  os_n_pending_reads.fetch_add(1, std::memory_order_relaxed);

  auto n_bytes = pread(file, buf, (ssize_t)n, off);

  os_file_n_pending_preads.fetch_sub(1, std::memory_order_relaxed);

  os_n_pending_reads.fetch_sub(1, std::memory_order_relaxed);

  return n_bytes;
}

/**
 * Does a synchronous write operation in Posix.
 *
 * @param[in] file Handle to a file.
 * @param[in] buf Buffer from where to write.
 * @param[in] n Number of bytes to write.
 * @param[in] offset Least significant 32 bits of file offset where to write.
 * @param[in] offset_high Most significant 32 bits of offset.
 *
 * @return Number of bytes written, -1 if error.
 */
static ssize_t os_file_pwrite(os_file_t file, const void *buf, ulint n, off_t off) {
  ++os_n_file_writes;

  os_file_n_pending_pwrites.fetch_add(1, std::memory_order_relaxed);

  os_n_pending_writes.fetch_add(1, std::memory_order_relaxed);

  auto n_bytes = pwrite(file, buf, (ssize_t)n, off);

  os_file_n_pending_pwrites.fetch_sub(1, std::memory_order_relaxed);

  os_n_pending_writes.fetch_sub(1, std::memory_order_relaxed);

  return n_bytes;
}

bool os_file_read(os_file_t file, void *p, ulint n, off_t off) {
  auto ptr = static_cast<char*>(p);

  os_bytes_read_since_printout += n;

  do {
    auto n_bytes = os_file_pread(file, ptr, n, off);

    if (n_bytes == -1) {
      switch (errno) {
        case EINTR:
        case EAGAIN:
          continue;
        default:
          if (os_file_handle_error(nullptr, "read")) {
            continue;
          } else {
            return false;
          }
      }
    }

    n -= n_bytes;
    ptr += n_bytes;
    off += n_bytes;
  } while (n > 0);

  return true;
}

bool os_file_read_no_error_handling(os_file_t file, void *p, ulint n, off_t off) {
  auto ptr = static_cast<char*>(p);

  os_bytes_read_since_printout += n;

  do {
    auto n_bytes = os_file_pread(file, ptr, n, off);

    if (n_bytes == -1) {
      switch (errno) {
        case EINTR:
        case EAGAIN:
          continue;
        default:
          if (os_file_handle_error_no_exit(nullptr, "read")) {
            continue;
          } else {
            return false;
          }
      }
    }
    n -= n_bytes;
    ptr += n_bytes;
    off += n_bytes;
  } while (n > 0);

  return true;
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

bool os_file_write(const char *name, os_file_t file, const void *p, ulint n, off_t off) {
  auto ptr = static_cast<const char*>(p);

  do {
    auto n_bytes = os_file_pwrite(file, ptr, n, off);

    if (n_bytes == -1) {
      switch (errno) {
        case EINTR:
        case EAGAIN:
          continue;
        default:
          if (os_file_handle_error(name, "write")) {
            if (!os_has_said_disk_full) {
              log_err(
                std::format("Write to file {} failed at offset {}. {} bytes should have been written,"
                            " only {} were written. Operating system error number {} - '{}'."
                            " Check that the disk is not full or a disk quota exceeded.",
                            name, off, n, n_bytes, errno, strerror(errno)));
            }

            os_has_said_disk_full = true;
            continue;

          } else {
            return false;
          }
      }
    }

    n -= n_bytes;
    ptr += n_bytes;
    off += n_bytes;

  } while (n > 0);

  return true;
}

bool os_file_status(const char *path, bool *exists, os_file_type_t *type) {
  struct stat statinfo;

  int ret = stat(path, &statinfo);

  if (ret != 0 && (errno == ENOENT || errno == ENOTDIR)) {
    /* File does not exist */
    *exists = false;
    return true;
  } else if (ret != 0) {
    /* File exists, but stat call failed */

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
  struct stat statinfo;

  int ret = stat(path, &statinfo);

  if (ret != 0 && (errno == ENOENT || errno == ENOTDIR)) {
    /* File does not exist */

    return false;
  } else if (ret != 0) {
    /* File exists, but stat call failed */

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

char *os_file_dirname(const char *path) {
  /* Find the offset of the last slash */
  const char *last_slash = strrchr(path, SRV_PATH_SEPARATOR);

  if (last_slash == nullptr) {
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
  bool subdir_exists;
  auto subdir = os_file_dirname(path);

  if (strlen(subdir) == 1 && (*subdir == SRV_PATH_SEPARATOR || *subdir == '.')) {
    /* subdir is root or cwd, nothing to do */
    mem_free(subdir);

    return true;
  }

  /* Test if subdir exists */
  os_file_type_t type;
  auto success = os_file_status(subdir, &subdir_exists, &type);

  if (success && !subdir_exists) {
    /* Subdir does not exist, create it */
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

/**
 * Creates an aio wait array.
 *
 * @param n         in: maximum number of pending aio operations allowed; n must be divisible by n_segments
 * @param n_segments    in: number of segments in the aio array
 * @return      own: aio array
 */
static AIO_array *os_aio_array_create(ulint n, ulint n_segments) {
  ut_a(n > 0);
  ut_a(n_segments > 0);

  auto ptr = ut_new(sizeof(AIO_array));
  auto array = new (ptr) AIO_array(n_segments, n);

  return array;
}

void os_aio_init(ulint n_per_seg, ulint n_read_segs, ulint n_write_segs, ulint n_slots_sync) {
  ulint n_segments = 1 + n_read_segs + n_write_segs;

  ut_ad(n_segments >= 3);

  os_io_init_simple();

  for (ulint i = 0; i < n_segments; i++) {
    os_set_io_thread_op_info(i, "not started yet");
  }

  os_aio_log_array = os_aio_array_create(n_per_seg, 1);

  srv_io_thread_function[0] = "log thread";

  os_aio_read_array = os_aio_array_create(n_read_segs * n_per_seg, n_read_segs);

  for (ulint i = 1; i < 1 + n_read_segs; i++) {
    ut_a(i < SRV_MAX_N_IO_THREADS);
    srv_io_thread_function[i] = "read thread";
  }

  os_aio_write_array = os_aio_array_create(n_write_segs * n_per_seg, n_write_segs);

  for (ulint i = 1 + n_read_segs; i < n_segments; i++) {
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
  os_event_wait(os_aio_write_array->m_is_empty);
}

ulint AIO_segment::get_local_segment_no(const AIO_slot *slot) const {
  if (m_array == os_aio_log_array) {
    return 0;

  } else if (m_array == os_aio_read_array) {
    auto seg_len = m_array->m_slots.capacity() / os_aio_read_array->m_n_segments;

    return 1 + slot->m_pos / seg_len;

  } else {
    ut_a(m_array == os_aio_write_array);

    auto seg_len = m_array->m_slots.capacity() / os_aio_write_array->m_n_segments;

    return 1 + os_aio_read_array->m_n_segments + slot->m_pos / seg_len;
  }
}

/**
 * Calculates local segment number and aio array from global segment number.
 *
 * @param global_segment    in: global segment number
 *
 * @return the AIO_segment with the local segment number and the aio array.
 */
AIO_segment AIO_segment::get_local_segment(ulint global_segment) {
  AIO_segment segment{};

  ut_a(global_segment < os_aio_n_segments);

  if (global_segment == 0) {

    segment.m_no = 0;
    segment.m_array = os_aio_log_array;

  } else if (global_segment < os_aio_read_array->m_n_segments + 1) {

    segment.m_no = global_segment - 1;
    segment.m_array = os_aio_read_array;

  } else {

    segment.m_array = os_aio_write_array;
    segment.m_no  = global_segment - (os_aio_read_array->m_n_segments + 1);

  }

  return segment;
}

AIO_slot *AIO_segment::reserve_slot(const IO_ctx &io_ctx, void *ptr, ulint len, off_t off) {
  /* No need of a mutex. Only reading constant fields */
  /* We attempt to keep adjacent blocks in the same local
  segment. This can help in merging IO requests when we are
  doing simulated AIO */
  auto slots_per_seg = get_slot_count();
  auto local_seg = (off >> (UNIV_PAGE_SIZE_SHIFT + 6)) % m_array->m_n_segments;
  auto slot_prepare = [&](AIO_slot &slot) {
    auto io_ctx_copy = io_ctx;
    ut_a(slot.m_reserved == false);

    ++m_array->m_n_reserved;

    if (m_array->m_n_reserved == 1) {
      os_event_reset(m_array->m_is_empty);
    }

    if (m_array->m_n_reserved == m_array->m_slots.capacity()) {
      os_event_reset(m_array->m_not_full);
    }

    slot.m_reserved = true;
    slot.m_reservation_time = time(nullptr);
    slot.m_io_ctx = std::move(io_ctx_copy);
    slot.m_len = len;
    slot.m_buf = static_cast<byte *>(ptr);
    slot.m_off = off;
    slot.m_io_already_done = false;
  };

  for (;;) {
    lock();

    if (m_array->m_n_reserved == m_array->m_slots.capacity()) {
      unlock();

      /* If the handler threads are suspended, wake them
      so that we get more slots */

      os_aio_simulated_wake_handler_threads();

      os_event_wait(m_array->m_not_full);

    } else {
      /* First try to find a slot in the preferred local segment */
      for (ulint i = local_seg * slots_per_seg; i < m_array->m_slots.capacity(); i++) {
        auto &slot = m_array->m_slots[i];

        if (!slot.m_reserved) {
          slot_prepare(slot); 
          unlock();
          return &slot;
        }
      }

      /* Fall back to a full scan. We are guaranteed to find a slot */
      for (auto &slot : m_array->m_slots) { 
        if (!slot.m_reserved) {
          slot_prepare(slot);
          unlock();
          return &slot;
        }
      }
    }
  }

  ut_error;
  return nullptr;
}

AIO_segment AIO_segment::get_segment(const IO_ctx& io_ctx) {
  AIO_segment segment;

  if (io_ctx.is_log_request()) {
    segment.m_array = os_aio_log_array;
  } else if (io_ctx.is_sync_request()) {
    segment.m_array = os_aio_sync_array;
  } else if (io_ctx.is_read_request()) {
    segment.m_array = os_aio_read_array;
  } else {
    segment.m_array = os_aio_read_array;
  } 

  return segment;
}

void AIO_segment::free_slot(AIO_slot *slot) {
  lock();

  ut_ad(slot->m_reserved);

  slot->m_reserved = false;

  --m_array->m_n_reserved;

  if (m_array->m_n_reserved == m_array->m_slots.capacity() - 1) {
    os_event_set(m_array->m_not_full);
  }

  if (m_array->m_n_reserved == 0) {
    os_event_set(m_array->m_is_empty);
  }

  unlock();
}

/**
 * Wakes up a simulated aio i/o-handler thread if it has something to do.
 *
 * @param global_segment - the number of the segment in the aio arrays
 */
static void os_aio_simulated_wake_handler_thread(ulint global_segment) {
  auto segment = AIO_segment::get_local_segment(global_segment);
  auto n = segment.get_slot_count();

  /* Look through n slots after the segment * n'th slot */

  segment.lock();

  bool found_empty_slot = false;

  for (ulint i = 0; i < n; i++) {
    auto slot = &segment.m_array->m_slots[i + segment.m_no * n];

    if (slot->m_reserved) {
      found_empty_slot = true;
      break;
    }
  }

  segment.unlock();

  if (found_empty_slot) {
    os_event_set(os_aio_segment_wait_events[global_segment]);
  }
}

void os_aio_simulated_wake_handler_threads() {
  os_aio_recommend_sleep_for_read_threads = false;

  for (ulint i = 0; i < os_aio_n_segments; i++) {
    os_aio_simulated_wake_handler_thread(i);
  }
}

void os_aio_simulated_put_read_threads_to_sleep() {}

bool os_aio(IO_ctx&& io_ctx, void *ptr, ulint n, off_t off) {
  ulint err = 0;

  io_ctx.validate();

  ut_ad(n > 0);
  ut_ad(ptr != nullptr);
  ut_ad(n % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(off % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(os_aio_validate());

  if (io_ctx.is_sync_request()) {
    if (io_ctx.is_read_request()) {
      return os_file_read(io_ctx.m_file, ptr, n, off);
    } else {
      return os_file_write(io_ctx.m_name, io_ctx.m_file, ptr, n, off);
    }
  }
  auto req_descr = io_ctx.is_read_request() ? "aio read" : "aio write";

  do {
    auto segment = AIO_segment::get_segment(io_ctx);
    auto slot = segment.reserve_slot(io_ctx, ptr, n, off);

    if (io_ctx.is_read_request()) {
      if (!io_ctx.m_batch) {
        os_aio_simulated_wake_handler_thread(segment.get_local_segment_no(slot));
      }
    } else {
      if (!io_ctx.m_batch) {
        os_aio_simulated_wake_handler_thread(segment.get_local_segment_no(slot));
      }
    }

    if (err == 0) {
      /* aio was queued successfully! */
      return true;
    }

    segment.free_slot(slot);

  } while (os_file_handle_error(io_ctx.m_name, req_descr));

  return false;
}

bool os_aio_simulated_handle(ulint global_segment, IO_ctx &out_io_ctx) {
  std::array<AIO_slot *, OS_AIO_MERGE_N_CONSECUTIVE> consecutive_ios;
  auto segment = AIO_segment::get_local_segment(global_segment);

  for (;;) {
    /* NOTE! We only access constant fields in os_aio_array. Therefore
    we do not have to acquire the protecting mutex yet */

    os_set_io_thread_op_info(global_segment, "looking for i/o requests (a)");
    ut_ad(os_aio_validate());
    ut_ad(segment.m_no < segment.m_array->m_n_segments);

    auto n = segment.get_slot_count();

    /* Look through n slots after the segment * n'th slot */

    if (segment.m_array == os_aio_read_array && os_aio_recommend_sleep_for_read_threads) {

      /* Give other threads chance to add several i/os to the array at once. */

      os_set_io_thread_op_info(global_segment, "waiting for i/o request");

      os_event_wait(os_aio_segment_wait_events[global_segment]);

      continue;
    }

    segment.lock();

    os_set_io_thread_op_info(global_segment, "looking for i/o requests (b)");

    /* Check if there is a slot for which the i/o has already been done */

    for (ulint i = 0; i < n; i++) {
      auto slot = &segment.m_array->m_slots[i + segment.m_no * n];

      if (slot->m_reserved && slot->m_io_already_done) {

        ut_a(slot->m_reserved);

        out_io_ctx = slot->m_io_ctx;

        segment.unlock();

        segment.free_slot(slot);

        return true;
      }
    }

    ulint age{};
    ulint n_consecutive = 0;
    ulint biggest_age = 0;
    off_t lowest_offset = std::numeric_limits<off_t>::max();

    /* If there are at least 2 seconds old requests, then pick the oldest
    one to prevent starvation. If several requests have the same age,
    then pick the one at the lowest offset. */

    for (ulint i = 0; i < n; i++) {
      auto slot = &segment.m_array->m_slots[i + segment.m_no * n];

      if (slot->m_reserved) {
        age = (ulint)difftime(time(nullptr), slot->m_reservation_time);

        if ((age >= 2 && age > biggest_age) ||
            (age >= 2 && age == biggest_age && slot->m_off < lowest_offset)) {

          /* Found an i/o request */
          consecutive_ios[0] = slot;

          n_consecutive = 1;

          biggest_age = age;
          lowest_offset = slot->m_off;
        }
      }
    }

    if (n_consecutive == 0) {
      /* There were no old requests. Look for an i/o request at the
      lowest offset in the array (we ignore the high 32 bits of the
      offset in these heuristics) */

      off_t lowest_offset = std::numeric_limits<off_t>::max();

      for (ulint i = 0; i < n; i++) {
        auto slot = &segment.m_array->m_slots[i + segment.m_no * n];

        if (slot->m_reserved && slot->m_off < lowest_offset) {
          /* Found an i/o request */
          consecutive_ios[0] = slot;

          n_consecutive = 1;

          lowest_offset = slot->m_off;
        }
      }
    }

    if (n_consecutive == 0) {
      /* No i/o requested at the moment */

      os_set_io_thread_op_info(global_segment, "resetting wait event");

      /* We wait here until there again can be i/os in the segment of this thread */

      os_event_reset(os_aio_segment_wait_events[global_segment]);

      segment.unlock();

      /* Give other threads chance to add several i/os to the array at once. */

      os_set_io_thread_op_info(global_segment, "waiting for i/o request");

      os_event_wait(os_aio_segment_wait_events[global_segment]);

      continue;
    }

    auto slot = consecutive_ios[0];
    ut_a(slot != nullptr);

    /* Check if there are several consecutive blocks to read or write */

    ulint i;

    do {
      for (i = 0; i < n && n_consecutive < OS_AIO_MERGE_N_CONSECUTIVE; i++) {
        auto slot2 = &segment.m_array->m_slots[i + segment.m_no * n];

        if (slot2->m_reserved &&
            slot2 != slot &&
            slot2->m_io_ctx.m_file  == slot->m_io_ctx.m_file &&
            slot2->m_off == slot->m_off + off_t(slot->m_len) &&
            slot2->m_io_ctx.m_io_request == slot->m_io_ctx.m_io_request) {

          /* Found a consecutive i/o request */
          consecutive_ios[n_consecutive] = slot2;

          ++n_consecutive;

          slot = slot2;

          /* Scan from the beginning comparing the current match. */
          break;
        }
      }
    } while (n_consecutive < OS_AIO_MERGE_N_CONSECUTIVE && i < n);

    os_set_io_thread_op_info(global_segment, "consecutive i/o requests");

    /* We have now collected n_consecutive i/o requests in the array;
    allocate a single buffer which can hold all data, and perform the
    i/o */

    uint total_len = 0;
    slot = consecutive_ios[0];

    for (ulint i = 0; i < n_consecutive; i++) {
      total_len += consecutive_ios[i]->m_len;
    }

    byte *combined_buf;
    byte *combined_buf_ptr;

    if (n_consecutive == 1) {
      /* We can use the buffer of the i/o request */
      combined_buf = slot->m_buf;
      combined_buf_ptr = nullptr;
    } else {
      combined_buf_ptr = static_cast<byte *>(ut_new(total_len + UNIV_PAGE_SIZE));

      ut_a(combined_buf_ptr);

      combined_buf = static_cast<byte *>(ut_align(combined_buf_ptr, UNIV_PAGE_SIZE));
    }

    /* We release the array mutex for the time of the i/o: NOTE that
    this assumes that there is just one i/o-handler thread serving
    a single segment of slots! */

    segment.unlock();

    ulint offs{};

    if (!slot->m_io_ctx.is_read_request() && n_consecutive > 1) {
      /* Copy the buffers to the combined buffer */
      offs = 0;

      for (ulint i = 0; i < n_consecutive; i++) {
        memcpy(combined_buf + offs, consecutive_ios[i]->m_buf, consecutive_ios[i]->m_len);
        offs += consecutive_ios[i]->m_len;
      }
    }

    os_set_io_thread_op_info(global_segment, "doing file i/o");

    bool ret;

    /* Do the i/o with ordinary, synchronous i/o functions: */
    if (slot->m_io_ctx.is_read_request()) {
      const auto &io_ctx = slot->m_io_ctx;

      ret = os_file_read(io_ctx.m_file, combined_buf, total_len, slot->m_off);
    } else {
      const auto &io_ctx = slot->m_io_ctx;

      ret = os_file_write(io_ctx.m_name, io_ctx.m_file, combined_buf, total_len, slot->m_off);
    }

    ut_a(ret);
    os_set_io_thread_op_info(global_segment, "file i/o done");

    if (slot->m_io_ctx.is_read_request() && n_consecutive > 1) {
      /* Copy the combined buffer to individual buffers */
      offs = 0;

      for (ulint i = 0; i < n_consecutive; i++) {
        memcpy(consecutive_ios[i]->m_buf, combined_buf + offs, consecutive_ios[i]->m_len);
        offs += consecutive_ios[i]->m_len;
      }
    }

    if (combined_buf_ptr) {
      ut_delete(combined_buf_ptr);
    }

    segment.lock();
    
    /* Mark the i/os done in slots */

    for (ulint i = 0; i < n_consecutive; i++) {
      consecutive_ios[i]->m_io_already_done = true;
    }

    ut_a(slot->m_reserved);

    out_io_ctx = slot->m_io_ctx;

    segment.unlock();
    segment.free_slot(slot);

    return ret;
  }
}

/**
 * Validates the consistency of an aio array.
 *
 * @param array - aio wait array
 *
 * @return true if ok
 */
static bool os_aio_array_validate(AIO_array *array) {
  os_mutex_enter(array->m_mutex);

  ut_a(!array->m_slots.empty());
  ut_a(array->m_n_segments > 0);

  ulint n_reserved{};

  for (auto &slot : array->m_slots) {
    if (slot.m_reserved) {
      ++n_reserved;
      ut_a(slot.m_len > 0);
    }
  }

  ut_a(array->m_n_reserved == n_reserved);

  os_mutex_exit(array->m_mutex);

  return true;
}

bool os_aio_validate() {
  os_aio_array_validate(os_aio_read_array);
  os_aio_array_validate(os_aio_write_array);
  os_aio_array_validate(os_aio_log_array);
  os_aio_array_validate(os_aio_sync_array);

  return true;
}

void os_aio_print(ib_stream_t ib_stream) {
  AIO_array *array;
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

  for (;;) {
    ut_a(array);

    os_mutex_enter(array->m_mutex);

    ut_a(!array->m_slots.empty());
    ut_a(array->m_n_segments > 0);

    n_reserved = 0;

    for (auto &slot : array->m_slots) {
      if (slot.m_reserved) {
        ++n_reserved;
        ut_a(slot.m_len > 0);
      }
    }

    ut_a(array->m_n_reserved == n_reserved);

    ib_logger(ib_stream, " %lu", (ulong)n_reserved);

    os_mutex_exit(array->m_mutex);

    if (array == os_aio_read_array) {
      ib_logger(ib_stream, ", aio writes:");

      array = os_aio_write_array;

      continue;
   }

    if (array == os_aio_write_array) {
      ib_logger(ib_stream, ",\n log reads:");
      array = os_aio_log_array;

      continue;
    }

    if (array == os_aio_log_array) {
      ib_logger(ib_stream, ", sync i/o's:");
      array = os_aio_sync_array;

      continue;
    }

    break;
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
    "%.2f reads/s, %lu avg bytes/read, %.2f writes/s, %.2f fsyncs/s\n",
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

  os_mutex_enter(array->m_mutex);

  n_res += array->m_n_reserved;

  os_mutex_exit(array->m_mutex);

  array = os_aio_write_array;

  os_mutex_enter(array->m_mutex);

  n_res += array->m_n_reserved;

  os_mutex_exit(array->m_mutex);

  array = os_aio_log_array;

  os_mutex_enter(array->m_mutex);

  n_res += array->m_n_reserved;

  os_mutex_exit(array->m_mutex);

  array = os_aio_sync_array;

  os_mutex_enter(array->m_mutex);

  n_res += array->m_n_reserved;

  os_mutex_exit(array->m_mutex);

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
