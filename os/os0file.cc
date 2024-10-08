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
#include "ut0mem.h"
#include "os0sync.h"
#include "os0thread.h"

/** Umask for creating files */
constexpr lint CREATE_MASK = S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP;

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

void os_file_var_init() {
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

void os_file_init() { /* Do nothing. */}

void os_file_free() { /* Do Nothing. */}

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
  bool retry;

  for (;;) {
    int create_flag;
    const char *mode_str = nullptr;

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
    if (type == OS_LOG_FILE && srv_config.m_unix_file_flush_method == SRV_UNIX_O_DSYNC) {
      create_flag = create_flag | O_SYNC;
    }
#endif /* O_SYNC */

    auto file = open(name, create_flag, CREATE_MASK);

    if (file == -1) {
      *success = false;

      /* When srv_file_per_table is on, file creation failure may not
      be critical to the whole instance. Do not crash the server in
      case of unknown errors. */
      if (srv_config.m_file_per_table) {
        retry = os_file_handle_error_no_exit(name, create_mode == OS_FILE_CREATE ? "create" : "open");
      } else {
        retry = os_file_handle_error(name, create_mode == OS_FILE_CREATE ? "create" : "open");
      }

      if (retry) {
       continue; 
      } else {
        return file /* -1 */;
      }
    }

    *success = true;

    /* We disable OS caching (O_DIRECT) only on data files */
    if (type != OS_LOG_FILE && srv_config.m_unix_file_flush_method == SRV_UNIX_O_DIRECT) {
      os_file_set_nocache(file, name, mode_str);
    }

    return file;
  }
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

bool os_file_set_size(const char *name, os_file_t file, off_t desired_size) {
  ut_a(desired_size >= off_t(UNIV_PAGE_SIZE));

  /* Write up to 1 megabyte at a time. */
  auto buf_size = ut_min(64, ulint(desired_size / UNIV_PAGE_SIZE)) * UNIV_PAGE_SIZE;
  auto ptr = static_cast<byte *>(ut_new(buf_size + UNIV_PAGE_SIZE));
  auto buf = static_cast<byte *>(ut_align(ptr, UNIV_PAGE_SIZE));

  /* Write buffer full of zeros */
  memset(buf, 0, buf_size);

  if (desired_size >= off_t(100 * 1024 * 1024)) {
    log_info_hdr("Progress in MB:");
  }

  off_t current_size;

  {
    auto success = os_file_get_size(file, &current_size);
    ut_a(success);
  }

  while (current_size < desired_size) {
    ulint n_bytes;

    if (desired_size - current_size < off_t(buf_size)) {
      n_bytes = ulint(desired_size - current_size);
    } else {
      n_bytes = buf_size;
    }

    auto ret = os_file_write(name, file, buf, n_bytes, current_size);

    if (!ret) {
      ut_delete(ptr);
      return false;
    }

    /* Print about progress for each 100 MB written */
    if (off_t(current_size + n_bytes) / off_t(100 * 1024 * 1024) != current_size / off_t(100 * 1024 * 1024)) {

      log_info_msg(std::format("{}00", ulong((current_size + n_bytes) / off_t(100 * 1024 * 1024))));
    }

    current_size += n_bytes;
  }

  if (desired_size >= off_t(100 * 1024 * 1024)) {
    log_info_msg("\n");
  }

  ut_delete(ptr);

  return os_file_flush(file);
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

void os_file_print(ib_stream_t ib_stream) {
  ib_logger(ib_stream, "\n");
  auto current_time = time(nullptr);
  auto time_elapsed = 0.001 + difftime(current_time, os_last_printout);

  ib_logger(
    ib_stream,
    "Pending flushes (fsync) log: %lu; buffer pool: %lu\n"
    "%lu OS file reads, %lu OS file writes, %lu OS fsyncs\n",
    (ulong)srv_fil->get_pending_log_flushes(),
    (ulong)srv_fil->get_pending_tablespace_flushes(),
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

  double avg_bytes_read;

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

void os_file_refresh_stats() {
  os_n_file_reads_old = os_n_file_reads;
  os_n_file_writes_old = os_n_file_writes;
  os_n_fsyncs_old = os_n_fsyncs;
  os_bytes_read_since_printout = 0;

  os_last_printout = time(nullptr);
}
