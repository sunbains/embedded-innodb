/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.
Copyright (c) 2009, Percona Inc.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

Portions of this file contain modifications contributed and copyrighted
by Percona Inc.. Those modifications are
gratefully acknowledged and are described briefly in the InnoDB
documentation. The contributions by Percona Inc. are incorporated with
their permission, and subject to the conditions contained in the file
COPYING.Percona.

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

/** @file srv/srv0start.c
Starts the InnoDB database server

Created 2/16/1996 Heikki Tuuri
*************************************************************************/

#include "srv0start.h"
#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0pcur.h"

#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "data0data.h"
#include "data0type.h"
#include "dict0boot.h"
#include "dict0crea.h"
#include "dict0dict.h"
#include "dict0load.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "lock0lock.h"
#include "log0log.h"
#include "log0recv.h"
#include "mem0mem.h"
#include "mtr0mtr.h"
#include "os0file.h"
#include "os0proc.h"
#include "os0sync.h"
#include "os0thread.h"
#include "page0cur.h"
#include "page0page.h"
#include "pars0pars.h"
#include "que0que.h"
#include "rem0cmp.h"
#include "rem0rec.h"
#include "row0ins.h"
#include "row0row.h"
#include "row0sel.h"
#include "row0upd.h"
#include "srv0srv.h"
#include "sync0sync.h"
#include "thr0loc.h"
#include "trx0purge.h"
#include "trx0roll.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "usr0sess.h"
#include "ut0mem.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <errno.h>

/* Log sequence number immediately after startup */
uint64_t srv_start_lsn;

/** Log sequence number at shutdown */
uint64_t srv_shutdown_lsn;

#ifdef HAVE_DARWIN_THREADS
#include <sys/utsname.h>
/** true if the F_FULLFSYNC option is available */
bool srv_have_fullfsync = false;
#endif /* HAVE_DARWIN_THREADS */

/** true if a raw partition is in use */
bool srv_start_raw_disk_in_use = false;

/** true if the server is being started, before rolling back any
incomplete transactions */
bool srv_startup_is_before_trx_rollback_phase = false;

/** true if the server is being started */
bool srv_is_being_started = false;

/** true if the server was successfully started */
bool srv_was_started = false;

/** true if innobase_start_or_create_for_mysql() has been called */
static bool srv_start_has_been_called = false;

/** At a shutdown this value climbs from SRV_SHUTDOWN_NONE to
SRV_SHUTDOWN_CLEANUP and then to SRV_SHUTDOWN_LAST_PHASE, and so on */
enum srv_shutdown_state srv_shutdown_state = SRV_SHUTDOWN_NONE;

/** Files comprising the system tablespace */
static os_file_t files[1000];

/** Mutex protecting the ios count */
static mutex_t ios_mutex;

/** Count of I/O operations in io_handler_thread() */
static ulint ios;

/** io_handler_thread parameters for thread identification */
static ulint n[SRV_MAX_N_IO_THREADS + 6];

/** io_handler_thread identifiers */
static os_thread_id_t thread_ids[SRV_MAX_N_IO_THREADS + 6];

/** The value passed to srv_parse_data_file_paths_and_sizes() is copied to
this variable. Since the function does a destructive read. */
static char *data_path_buf;

/** The value passed to srv_parse_log_group_home_dirs() is copied to
this variable. Since the function does a destructive read. */
static char *log_path_buf;

/**We use this mutex to test the return value of pthread_mutex_trylock
on successful locking. HP-UX does NOT return 0, though Linux et al do. */
static os_fast_mutex_t srv_os_test_mutex;

/** The system data file names */
static char **srv_data_file_names = nullptr;

/** The system log file names */
static char **srv_log_group_home_dirs = nullptr;

/** All threads end up waiting for certain events. Put those events
to the signaled state. Then the threads will exit themselves in
os_thread_event_wait().
@return	true if all threads exited. */
static bool srv_threads_shutdown(void);

#define SRV_N_PENDING_IOS_PER_THREAD OS_AIO_N_PENDING_IOS_PER_THREAD
constexpr ulint SRV_MAX_N_PENDING_SYNC_IOS = 100;

/** Convert a numeric string that optionally ends in G or M, to a number
containing megabytes.
@return	next character in string */
static char *srv_parse_megabytes(
  char *str, /*!< in: string containing a quantity in bytes */
  ulint *megs
) /*!< out: the number in megabytes */
{
  char *endp;
  ulint size;

  size = strtoul(str, &endp, 10);

  str = endp;

  switch (*str) {
    case 'G':
    case 'g':
      size *= 1024;
      /* fall through */
    case 'M':
    case 'm':
      str++;
      break;
    default:
      size /= 1024 * 1024;
      break;
  }

  *megs = size;
  return str;
}

/** Adds a slash or a backslash to the end of a string if it is missing
and the string is not empty.
@param[in] str                  Nul terminated char array.
@return	string which has the separator if the string is not empty.
        The string is alloc'ed using malloc() and the caller is
        responsible for freeing the string. */
static char *srv_add_path_separator_if_needed(const char *str) {
  auto len = strlen(str);
  auto out_str = static_cast<char *>(malloc(len + 2));

  std::strcpy(out_str, str);

  if (len > 0 && out_str[len - 1] != SRV_PATH_SEPARATOR) {
    out_str[len] = SRV_PATH_SEPARATOR;
    out_str[len + 1] = 0;
  }

  return out_str;
}

bool srv_parse_data_file_paths_and_sizes(const char *usr_str) {
  char *str;
  char *path;
  ulint size;
  char *input_str;
  ulint i = 0;

  if (data_path_buf != nullptr) {
    free(data_path_buf);
    data_path_buf = nullptr;
  }

  data_path_buf = static_cast<char *>(malloc(strlen(usr_str) + 1));
  strcpy(data_path_buf, usr_str);
  str = data_path_buf;

  srv_auto_extend_last_data_file = false;
  srv_last_file_size_max = 0;
  if (srv_data_file_names != nullptr) {
    free(srv_data_file_names);
    srv_data_file_names = nullptr;
  }
  if (srv_data_file_sizes != nullptr) {
    free(srv_data_file_sizes);
    srv_data_file_sizes = nullptr;
  }
  if (srv_data_file_is_raw_partition != nullptr) {
    free(srv_data_file_is_raw_partition);
    srv_data_file_is_raw_partition = nullptr;
  }

  input_str = str;

  /* First calculate the number of data files and check syntax:
  path:size[M | G];path:size[M | G]... . Note that a Windows path may
  contain a drive name and a ':'. */

  while (*str != '\0') {
    path = str;

    while ((*str != ':' && *str != '\0') || (*str == ':' && (*(str + 1) == '\\' || *(str + 1) == '/' || *(str + 1) == ':'))) {
      str++;
    }

    if (*str == '\0') {
      return false;
    }

    str++;

    str = srv_parse_megabytes(str, &size);

    if (0 == strncmp(str, ":autoextend", (sizeof ":autoextend") - 1)) {

      str += (sizeof ":autoextend") - 1;

      if (0 == strncmp(str, ":max:", (sizeof ":max:") - 1)) {

        str += (sizeof ":max:") - 1;

        str = srv_parse_megabytes(str, &size);
      }

      if (*str != '\0') {

        return false;
      }
    }

    if (strlen(str) >= 6 && *str == 'n' && *(str + 1) == 'e' && *(str + 2) == 'w') {
      str += 3;
    }

    if (*str == 'r' && *(str + 1) == 'a' && *(str + 2) == 'w') {
      str += 3;
    }

    if (size == 0) {
      return false;
    }

    i++;

    if (*str == ';') {
      str++;
    } else if (*str != '\0') {

      return false;
    }
  }

  if (i == 0) {
    /* If data_file_path was defined it must contain
    at least one data file definition */

    return false;
  }

  ut_a(srv_data_file_names == nullptr);
  srv_data_file_names = static_cast<char **>(malloc(i * sizeof(*srv_data_file_names)));

  ut_a(srv_data_file_sizes == nullptr);
  srv_data_file_sizes = static_cast<ulint *>(malloc(i * sizeof(ulint)));

  ut_a(srv_data_file_is_raw_partition == nullptr);
  srv_data_file_is_raw_partition = static_cast<ulint *>(malloc(i * sizeof(ulint)));

  srv_n_data_files = i;

  /* Then store the actual values to our arrays */

  str = input_str;
  i = 0;

  while (*str != '\0') {
    path = str;

    /* Note that we must step over the ':' in a Windows path;
    a Windows path normally looks like C:\ibdata\ibdata1:1G, but
    a Windows raw partition may have a specification like
    \\.\C::1Gnewraw or \\.\PHYSICALDRIVE2:1Gnewraw */

    while ((*str != ':' && *str != '\0') || (*str == ':' && (*(str + 1) == '\\' || *(str + 1) == '/' || *(str + 1) == ':'))) {
      str++;
    }

    if (*str == ':') {
      /* Make path a null-terminated string */
      *str = '\0';
      str++;
    }

    str = srv_parse_megabytes(str, &size);

    srv_data_file_names[i] = path;
    srv_data_file_sizes[i] = size;

    if (0 == strncmp(str, ":autoextend", (sizeof ":autoextend") - 1)) {

      srv_auto_extend_last_data_file = true;

      str += (sizeof ":autoextend") - 1;

      if (0 == strncmp(str, ":max:", (sizeof ":max:") - 1)) {

        str += (sizeof ":max:") - 1;

        str = srv_parse_megabytes(str, &srv_last_file_size_max);
      }

      if (*str != '\0') {

        return false;
      }
    }

    (srv_data_file_is_raw_partition)[i] = 0;

    if (strlen(str) >= 6 && *str == 'n' && *(str + 1) == 'e' && *(str + 2) == 'w') {
      str += 3;
      (srv_data_file_is_raw_partition)[i] = SRV_NEW_RAW;
    }

    if (*str == 'r' && *(str + 1) == 'a' && *(str + 2) == 'w') {
      str += 3;

      if ((srv_data_file_is_raw_partition)[i] == 0) {
        (srv_data_file_is_raw_partition)[i] = SRV_OLD_RAW;
      }
    }

    i++;

    if (*str == ';') {
      str++;
    }
  }

  return true;
}

/** Reads log group home directories from a character string.
@return	true if ok, false on parse error */

bool srv_parse_log_group_home_dirs(const char *usr_str) /*!< in: character string */
{
  ulint i;
  char *str;
  char *path;
  int n_bytes;
  char *input_str;

  if (log_path_buf != nullptr) {
    free(log_path_buf);
    log_path_buf = nullptr;
  }

  log_path_buf = static_cast<char *>(malloc(strlen(usr_str) + 1));

  strcpy(log_path_buf, usr_str);
  str = log_path_buf;

  if (srv_log_group_home_dirs != nullptr) {

    for (i = 0; srv_log_group_home_dirs[i] != nullptr; ++i) {
      free(srv_log_group_home_dirs[i]);
      srv_log_group_home_dirs[i] = nullptr;
    }

    free(srv_log_group_home_dirs);
    srv_log_group_home_dirs = nullptr;
  }

  i = 0;
  input_str = str;

  /* First calculate the number of directories and check syntax:
  path;path;... */

  while (*str != '\0') {
    path = str;

    while (*str != ';' && *str != '\0') {
      str++;
    }

    i++;

    if (*str == ';') {
      str++;
    } else if (*str != '\0') {

      return false;
    }
  }

  if (i != 1) {
    /* If log_group_home_dir was defined it must
    contain exactly one path definition. */

    return false;
  }

  /* Add sentinel element to the array. */
  n_bytes = (i + 1) * sizeof(*srv_log_group_home_dirs);
  srv_log_group_home_dirs = static_cast<char **>(malloc(n_bytes));
  memset(srv_log_group_home_dirs, 0x0, n_bytes);

  /* Then store the actual values to our array */

  str = input_str;
  i = 0;

  while (*str != '\0') {
    path = str;

    while (*str != ';' && *str != '\0') {
      str++;
    }

    if (*str == ';') {
      *str = '\0';
      str++;
    }

    /* Note that this memory is malloc() and so must be freed. */
    srv_log_group_home_dirs[i] = srv_add_path_separator_if_needed(path);

    i++;
  }

  /* We rely on this sentinel value during free. */
  ut_a(i > 0);
  ut_a(srv_log_group_home_dirs[i] == nullptr);

  return true;
}

/** Frees the memory allocated by srv_parse_data_file_paths_and_sizes()
and srv_parse_log_group_home_dirs(). */

void srv_free_paths_and_sizes(void) {
  if (srv_data_file_names != nullptr) {
    free(srv_data_file_names);
    srv_data_file_names = nullptr;
  }

  if (srv_data_file_sizes != nullptr) {
    free(srv_data_file_sizes);
    srv_data_file_sizes = nullptr;
  }

  if (srv_data_file_is_raw_partition != nullptr) {
    free(srv_data_file_is_raw_partition);
    srv_data_file_is_raw_partition = nullptr;
  }

  if (srv_log_group_home_dirs != nullptr) {
    ulint i;

    for (i = 0; srv_log_group_home_dirs[i] != nullptr; ++i) {
      free(srv_log_group_home_dirs[i]);
      srv_log_group_home_dirs[i] = nullptr;
    }
    free(srv_log_group_home_dirs);
    srv_log_group_home_dirs = nullptr;
  }

  if (data_path_buf != nullptr) {
    free(data_path_buf);
    data_path_buf = nullptr;
  }

  if (log_path_buf != nullptr) {
    free(log_path_buf);
    log_path_buf = nullptr;
  }
}

void *io_handler_thread(void *arg) {
  auto segment = *((ulint *)arg);

#ifdef UNIV_DEBUG_THREAD_CREATION
  ib_logger(ib_stream, "Io handler thread %lu starts, id %lu\n", segment, os_thread_pf(os_thread_get_curr_id()));
#endif

  for (;;) {
    fil_aio_wait(segment);

    mutex_enter(&ios_mutex);
    ios++;
    mutex_exit(&ios_mutex);
  }

  thr_local_free(os_thread_get_curr_id());

  /* We count the number of threads in os_thread_exit(). A created
  thread should always use that to exit and not use return() to exit.
  The thread actually never comes here because it is exited in an
  os_event_wait(). */

  os_thread_exit(nullptr);

  return nullptr;
}

/** Calculates the low 32 bits when a file size which is given as a number
database pages is converted to the number of bytes.
@return	low 32 bytes of file size when expressed in bytes */
static ulint srv_calc_low32(ulint file_size) /*!< in: file size in database pages */
{
  return 0xFFFFFFFFUL & (file_size << UNIV_PAGE_SIZE_SHIFT);
}

/** Calculates the high 32 bits when a file size which is given as a number
database pages is converted to the number of bytes.
@return	high 32 bytes of file size when expressed in bytes */
static ulint srv_calc_high32(ulint file_size) /*!< in: file size in database pages */
{
  return file_size >> (32 - UNIV_PAGE_SIZE_SHIFT);
}

/** Creates or opens the log files and closes them.
@return	DB_SUCCESS or error code */
static db_err open_or_create_log_file(
  bool create_new_db,            /*!< in: true if we should create a
                                    new database */
  bool *log_file_created,        /*!< out: true if new log file
                                    created */
  bool log_file_has_been_opened, /*!< in: true if a log file has been
                                   opened before: then it is an error
                                   to try to create another log file */
  ulint k,                       /*!< in: log group number */
  ulint i
) /*!< in: log file number in group */
{
  bool ret;
  ulint size;
  ulint size_high;
  char name[10000];

  UT_NOT_USED(create_new_db);

  *log_file_created = false;

  ut_a(strlen(srv_log_group_home_dirs[k]) < (sizeof name) - 10 - sizeof "ib_logfile");

  ut_snprintf(name, sizeof(name), "%s%s%lu", srv_log_group_home_dirs[k], "ib_logfile", (ulong)i);

  files[i] = os_file_create(name, OS_FILE_CREATE, OS_FILE_NORMAL, OS_LOG_FILE, &ret);

  if (ret == false) {
    if (os_file_get_last_error(false) != OS_FILE_ALREADY_EXISTS) {
      ib_logger(
        ib_stream,
        "Error in creating"
        " or opening %s\n",
        name
      );

      return DB_ERROR;
    }

    files[i] = os_file_create(name, OS_FILE_OPEN, OS_FILE_AIO, OS_LOG_FILE, &ret);

    if (!ret) {
      ib_logger(ib_stream, "Error in opening %s\n", name);

      return DB_ERROR;
    }

    ret = os_file_get_size(files[i], &size, &size_high);
    ut_a(ret);

    if (size != srv_calc_low32(srv_log_file_size) || size_high != srv_calc_high32(srv_log_file_size)) {

      ib_logger(
        ib_stream,
        "Error: log file %s is"
        " of different size %lu %lu bytes\n"
        "than the configured %lu %lu bytes!\n",
        name,
        (ulong)size_high,
        (ulong)size,
        (ulong)srv_calc_high32(srv_log_file_size),
        (ulong)srv_calc_low32(srv_log_file_size)
      );

      return DB_ERROR;
    }
  } else {
    *log_file_created = true;

    ut_print_timestamp(ib_stream);

    ib_logger(
      ib_stream,
      "  Log file %s did not exist:"
      " new to be created\n",
      name
    );
    if (log_file_has_been_opened) {

      return DB_ERROR;
    }

    ib_logger(ib_stream, "Setting log file %s size to %lu MB\n", name, (ulong)srv_log_file_size >> (20 - UNIV_PAGE_SIZE_SHIFT));

    ib_logger(
      ib_stream,
      "Database physically writes the file"
      " full: wait...\n"
    );

    ret = os_file_set_size(name, files[i], srv_calc_low32(srv_log_file_size), srv_calc_high32(srv_log_file_size));
    if (!ret) {
      ib_logger(
        ib_stream,
        "Error in creating %s:"
        " probably out of disk space\n",
        name
      );

      return DB_ERROR;
    }
  }

  ret = os_file_close(files[i]);
  ut_a(ret);

  if (i == 0) {
    /* Create in memory the file space object which is for this log group */

    fil_space_create(name, 2 * k + SRV_LOG_SPACE_FIRST_ID, 0, FIL_LOG);
  }

  ut_a(fil_validate());

  fil_node_create(name, srv_log_file_size, 2 * k + SRV_LOG_SPACE_FIRST_ID, false);

  if (i == 0) {
    log_group_init(k, srv_n_log_files, srv_log_file_size * UNIV_PAGE_SIZE, 2 * k + SRV_LOG_SPACE_FIRST_ID);
  }

  return DB_SUCCESS;
}

/** Creates or opens database data files and closes them.
@return	DB_SUCCESS or error code */
static db_err open_or_create_data_files(
  bool *create_new_db,       /*!< out: true if new database should be
                          created */
  uint64_t *min_flushed_lsn, /*!< out: min of flushed lsn
                                  values in data files */
  uint64_t *max_flushed_lsn, /*!< out: max of flushed lsn
                                  values in data files */
  ulint *sum_of_new_sizes
) /*!< out: sum of sizes of the
                              new files added */
{
  bool ret;
  ulint i;
  bool one_opened = false;
  bool one_created = false;
  ulint size;
  ulint size_high;
  ulint rounded_size_pages;
  char name[PATH_MAX];
  char home[1024];

  if (srv_n_data_files >= 1000) {
    ib_logger(
      ib_stream,
      "can only have < 1000 data files\n"
      "you have defined %lu\n",
      (ulong)srv_n_data_files
    );
    return DB_ERROR;
  }

  *sum_of_new_sizes = 0;

  *create_new_db = false;

  /* Copy the path because we want to normalize it. */
  strcpy(home, srv_data_home);

  /* We require that the user have a trailing '/' when setting the
  srv_data_home variable. */

  for (i = 0; i < srv_n_data_files; i++) {
    bool is_absolute = false;
    const char *ptr = srv_data_file_names[i];

    /* We assume Unix file paths here. */
    is_absolute = (*ptr == '/');

    /* If the name is not absolute then the system files
    are created relative to home. */
    if (!is_absolute) {

      ut_a(strlen(home) + strlen(srv_data_file_names[i]) < (sizeof name) - 1);

      ut_snprintf(name, sizeof(name), "%.1024s%.1024s", home, srv_data_file_names[i]);
    } else {
      ut_a(strlen(home) < (sizeof name) - 1);

      ut_snprintf(name, sizeof(name), "%s", srv_data_file_names[i]);
    }

    if (srv_data_file_is_raw_partition[i] == 0) {

      /* First we try to create the file: if it already
      exists, ret will get value false */

      files[i] = os_file_create(name, OS_FILE_CREATE, OS_FILE_NORMAL, OS_DATA_FILE, &ret);

      if (
        ret == false &&
        os_file_get_last_error(false) != OS_FILE_ALREADY_EXISTS
#ifdef UNIV_AIX
        /* AIX 5.1 after security patch ML7 may have
          errno set to 0 here, which causes our function
          to return 100; work around that AIX problem */
        && os_file_get_last_error(false) != 100
#endif
      ) {
        ib_logger(
          ib_stream,
          "Error in creating"
          " or opening %s\n",
          name
        );

        return DB_ERROR;
      }
    } else if (srv_data_file_is_raw_partition[i] == SRV_NEW_RAW) {
      /* The partition is opened, not created; then it is
      written over */

      srv_start_raw_disk_in_use = true;
      srv_created_new_raw = true;

      files[i] = os_file_create(name, OS_FILE_OPEN_RAW, OS_FILE_NORMAL, OS_DATA_FILE, &ret);
      if (!ret) {
        ib_logger(ib_stream, "Error in opening %s\n", name);

        return DB_ERROR;
      }
    } else if (srv_data_file_is_raw_partition[i] == SRV_OLD_RAW) {
      srv_start_raw_disk_in_use = true;

      ret = false;
    } else {
      ut_a(0);
    }

    if (ret == false) {
      /* We open the data file */

      if (one_created) {
        ib_logger(
          ib_stream,
          "Error: data files can only"
          " be added at the end\n"
        );
        ib_logger(
          ib_stream,
          "of a tablespace, but"
          " data file %s existed beforehand.\n",
          name
        );
        return DB_ERROR;
      }

      if (srv_data_file_is_raw_partition[i] == SRV_OLD_RAW) {
        files[i] = os_file_create(name, OS_FILE_OPEN_RAW, OS_FILE_NORMAL, OS_DATA_FILE, &ret);
      } else if (i == 0) {
        files[i] = os_file_create(name, OS_FILE_OPEN_RETRY, OS_FILE_NORMAL, OS_DATA_FILE, &ret);
      } else {
        files[i] = os_file_create(name, OS_FILE_OPEN, OS_FILE_NORMAL, OS_DATA_FILE, &ret);
      }

      if (!ret) {
        ib_logger(ib_stream, "Error in opening %s\n", name);
        os_file_get_last_error(true);

        return DB_ERROR;
      }

      if (srv_data_file_is_raw_partition[i] == SRV_OLD_RAW) {

        goto skip_size_check;
      }

      ret = os_file_get_size(files[i], &size, &size_high);
      ut_a(ret);
      /* Round size downward to megabytes */

      rounded_size_pages = (size / (1024 * 1024) + 4096 * size_high) << (20 - UNIV_PAGE_SIZE_SHIFT);

      if (i == srv_n_data_files - 1 && srv_auto_extend_last_data_file) {

        if (srv_data_file_sizes[i] > rounded_size_pages || (srv_last_file_size_max > 0 && srv_last_file_size_max < rounded_size_pages)) {

          ib_logger(
            ib_stream,
            "Error: auto-extending"
            " data file %s is"
            " of a different size\n"
            "%lu pages (rounded"
            " down to MB) than the "
            "configured\n"
            "initial %lu pages,"
            " max %lu (relevant if"
            " non-zero) pages!\n",
            name,
            (ulong)rounded_size_pages,
            (ulong)srv_data_file_sizes[i],
            (ulong)srv_last_file_size_max
          );

          return DB_ERROR;
        }

        srv_data_file_sizes[i] = rounded_size_pages;
      }

      if (rounded_size_pages != srv_data_file_sizes[i]) {

        ib_logger(
          ib_stream,
          "Error: data file %s"
          " is of a different size\n"
          "%lu pages"
          " (rounded down to MB)\n"
          "than the configured "
          "%lu pages!\n",
          name,
          (ulong)rounded_size_pages,
          (ulong)srv_data_file_sizes[i]
        );

        return DB_ERROR;
      }
    skip_size_check:
      fil_read_flushed_lsn_and_arch_log_no(files[i], one_opened, min_flushed_lsn, max_flushed_lsn);
      one_opened = true;
    } else {
      /* We created the data file and now write it full of
      zeros */

      one_created = true;

      if (i > 0) {
        ut_print_timestamp(ib_stream);
        ib_logger(
          ib_stream,
          "  Data file %s did not"
          " exist: new to be created\n",
          name
        );
      } else {
        ib_logger(
          ib_stream,
          "The first specified"
          " data file %s did not exist:\n"
          "a new database"
          " to be created!\n",
          name
        );
        *create_new_db = true;
      }

      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream, "  Setting file %s size to %lu MB\n", name, (ulong)(srv_data_file_sizes[i] >> (20 - UNIV_PAGE_SIZE_SHIFT))
      );

      ib_logger(
        ib_stream,
        "Database physically writes the"
        " file full: wait...\n"
      );

      ret = os_file_set_size(name, files[i], srv_calc_low32(srv_data_file_sizes[i]), srv_calc_high32(srv_data_file_sizes[i]));

      if (!ret) {
        ib_logger(
          ib_stream,
          "Error in creating %s:"
          " probably out of disk space\n",
          name
        );

        return DB_ERROR;
      }

      *sum_of_new_sizes = *sum_of_new_sizes + srv_data_file_sizes[i];
    }

    ret = os_file_close(files[i]);
    ut_a(ret);

    if (i == 0) {
      fil_space_create(name, 0, 0, FIL_TABLESPACE);
    }

    ut_a(fil_validate());

    fil_node_create(name, srv_data_file_sizes[i], 0, srv_data_file_is_raw_partition[i] != 0);
  }

  ios = 0;

  mutex_create(&ios_mutex, SYNC_NO_ORDER_CHECK);

  return DB_SUCCESS;
}

/** Abort the startup process and shutdown the minimum set of sub-systems
required to create files and. */
static void srv_startup_abort(enum db_err err) /* in: Current error code */
{
  /* This is currently required to inform the master thread only. Once
  we have contexts we can get rid of this global. */
  srv_fast_shutdown = IB_SHUTDOWN_NORMAL;

  /* For fatal errors we want to avoid writing to the data files. */
  if (err != DB_FATAL) {
    logs_empty_and_mark_files_at_shutdown(srv_force_recovery, srv_fast_shutdown);

    fil_close_all_files();
  }

  srv_threads_shutdown();

  log_shutdown();
  lock_sys_close();
  buf_close();
  fil_close();
  os_aio_close();

  log_mem_free();
  buf_mem_free();
}

ib_err_t innobase_start_or_create() {
  buf_pool_t *ret;
  bool create_new_db;
  bool log_file_created;
  bool log_created = false;
  bool log_opened = false;
  uint64_t min_flushed_lsn;
  uint64_t max_flushed_lsn;
  ulint sum_of_new_sizes;
  ulint sum_of_data_file_sizes;
  ulint tablespace_size_in_header;
  db_err err;
  ulint i;
  ulint io_limit;
  mtr_t mtr;
  bool srv_file_per_table_original_value;

  // FIXME:
  ib_stream = stderr;

  srv_file_per_table_original_value = srv_file_per_table;

#ifdef HAVE_DARWIN_THREADS
#ifdef F_FULLFSYNC
  /* This executable has been compiled on Mac OS X 10.3 or later.
  Assume that F_FULLFSYNC is available at run-time. */
  srv_have_fullfsync = true;
#else  /* F_FULLFSYNC */
  /* This executable has been compiled on Mac OS X 10.2
  or earlier.  Determine if the executable is running
  on Mac OS X 10.3 or later. */
  struct utsname utsname;
  if (uname(&utsname)) {
    ib_logger(ib_stream, "cannot determine Mac OS X version!\n");
  } else {
    srv_have_fullfsync = strcmp(utsname.release, "7.") >= 0;
  }
  if (!srv_have_fullfsync) {
    ib_logger(
      ib_stream,
      "On Mac OS X, fsync() may be"
      " broken on internal drives,\n"
      "making transactions unsafe!\n"
    );
  }
#endif /* F_FULLFSYNC */
#endif /* HAVE_DARWIN_THREADS */

  if (sizeof(ulint) != sizeof(void *)) {
    ib_logger(
      ib_stream,
      "Error: size of InnoDB's ulint is %lu,"
      " but size of void* is %lu.\n"
      "The sizes should be the same"
      " so that on a 64-bit platform you can\n"
      "allocate more than 4 GB of memory.",
      (ulong)sizeof(ulint),
      (ulong)sizeof(void *)
    );
  }

  /* System tables are created in tablespace 0.  Thus, we must
  temporarily clear srv_file_per_table.  This is ok, because the
  server will not accept connections (which could modify
  file_per_table) until this function has returned. */
  srv_file_per_table = false;
#ifdef UNIV_DEBUG
  ib_logger(ib_stream, "!!!!!!!! UNIV_DEBUG switched on !!!!!!!!!\n");
#endif

#ifdef UNIV_SYNC_DEBUG
  ib_logger(ib_stream, "!!!!!!!! UNIV_SYNC_DEBUG switched on !!!!!!!!!\n");
#endif

#ifdef UNIV_SEARCH_DEBUG
  ib_logger(ib_stream, "!!!!!!!! UNIV_SEARCH_DEBUG switched on !!!!!!!!!\n");
#endif

#ifdef UNIV_LOG_LSN_DEBUG
  ib_logger(ib_stream, "!!!!!!!! UNIV_LOG_LSN_DEBUG switched on !!!!!!!!!\n");
#endif /* UNIV_LOG_LSN_DEBUG */

  if (likely(srv_use_sys_malloc)) {
    ib_logger(ib_stream, "The InnoDB memory heap is disabled\n");
  }

  ib_logger(ib_stream, "" IB_ATOMICS_STARTUP_MSG "\n");

  /* Print an error message if someone tries to start up InnoDB a
  second time while it's already in state running. */
  if (srv_was_started && srv_start_has_been_called) {
    ib_logger(
      ib_stream,
      "Error: startup called second time"
      " during the process lifetime.\n"
      "more than once during"
      " the process lifetime.\n"
    );
  }

  srv_start_has_been_called = true;

#ifdef UNIV_DEBUG
  log_do_write = true;
#endif /* UNIV_DEBUG */
  /*	yydebug = true; */

  srv_is_being_started = true;
  srv_startup_is_before_trx_rollback_phase = true;

  /* Note that the call srv_boot() also changes the values of
  some variables to the units used by InnoDB internally */

  /* Set the maximum number of threads which can wait for a semaphore
  inside this is the 'sync wait array' size, as well as the
  maximum number of threads that can wait in the 'srv_conc array' for
  their time to enter InnoDB. */

  if (srv_buf_pool_size >= 1000 * 1024 * 1024) {

    /* If buffer pool is less than 1000 MB, assume fewer threads. */
    srv_max_n_threads = 50000;

  } else if (srv_buf_pool_size >= 8 * 1024 * 1024) {

    srv_max_n_threads = 10000;

  } else {

    /* saves several MB of memory, especially in 64-bit computers */
    srv_max_n_threads = 1000;
  }

  err = srv_boot();

  if (err != DB_SUCCESS) {

    return err;
  }

  /* file_io_threads used to be user settable, now it's just a
     sum of read_io_threads and write_io_threads */
  srv_n_file_io_threads = 2 + srv_n_read_io_threads + srv_n_write_io_threads;

  ut_a(srv_n_file_io_threads <= SRV_MAX_N_IO_THREADS);

  /* TODO: Investigate if SRV_N_PENDING_IOS_PER_THREAD (32) limit
  still applies to windows. */
  if (!os_aio_use_native_aio) {
    io_limit = 8 * SRV_N_PENDING_IOS_PER_THREAD;
  } else {
    io_limit = SRV_N_PENDING_IOS_PER_THREAD;
  }

#ifdef UNIV_DEBUG
  /* We have observed deadlocks with a 5MB buffer pool but
  the actual lower limit could very well be a little higher. */

  if (srv_buf_pool_size <= 5 * 1024 * 1024) {

    ib_logger(
      ib_stream,
      "Warning: Small buffer pool size "
      "(%luM), the flst_validate() debug function "
      "can cause a deadlock if the buffer pool fills up.\n",
      srv_buf_pool_size / 1024 / 1024
    );
  }
#endif

  if (srv_n_log_files * srv_log_file_size >= 262144) {
    ib_logger(
      ib_stream,
      "Error: combined size of log files"
      " must be < 4 GB\n"
    );

    return DB_ERROR;
  }

  sum_of_new_sizes = 0;

  for (i = 0; i < srv_n_data_files; i++) {
    if (sizeof(off_t) < 5 && srv_data_file_sizes[i] >= 262144) {
      ib_logger(
        ib_stream,
        "Error: file size must be < 4 GB"
        " with this binary\n"
        "and operating system combination,"
        " in some OS's < 2 GB\n"
      );

      return DB_ERROR;
    }
    sum_of_new_sizes += srv_data_file_sizes[i];
  }

  if (sum_of_new_sizes < 10485760 / UNIV_PAGE_SIZE) {
    ib_logger(
      ib_stream,
      "Error: tablespace size must be"
      " at least 10 MB\n"
    );

    return DB_ERROR;
  }

  os_aio_init(io_limit, srv_n_read_io_threads, srv_n_write_io_threads, SRV_MAX_N_PENDING_SYNC_IOS);

  fil_init(srv_file_per_table ? 50000 : 5000, srv_max_n_open_files);

  ret = buf_pool_init();

  if (ret == nullptr) {
    /* Shutdown all sub-systems that have been initialized. */
    fil_close();
    os_aio_close();

    ib_logger(
      ib_stream,
      "Fatal error: cannot allocate the memory"
      " for the buffer pool\n"
    );

    return DB_ERROR;
  }

  fsp_init();
  innobase_log_init();

  lock_sys_create(srv_lock_table_size);

  /* Create i/o-handler threads: */

  for (i = 0; i < srv_n_file_io_threads; i++) {
    n[i] = i;

    os_thread_create(io_handler_thread, n + i, thread_ids + i);
  }

  err = open_or_create_data_files(&create_new_db, &min_flushed_lsn, &max_flushed_lsn, &sum_of_new_sizes);
  if (err != DB_SUCCESS) {
    ib_logger(
      ib_stream,
      "Could not open or create data files.\n"
      "If you tried to add new data files,"
      " and it failed here,\n"
      "you should now set data_file_path"
      " back\n"
      "to what it was, and remove the"
      " new ibdata files InnoDB created\n"
      "in this failed attempt. InnoDB only wrote"
      " those files full of\n"
      "zeros, but did not yet use them in any way."
      " But be careful: do not\n"
      "remove old data files"
      " which contain your precious data!\n"
    );

    srv_startup_abort(err);

    return err;
  }

  for (i = 0; i < srv_n_log_files; i++) {

    err = open_or_create_log_file(create_new_db, &log_file_created, log_opened, 0, i);

    if (err != DB_SUCCESS) {
      srv_startup_abort(err);
      return err;
    } else if (log_file_created) {
      log_created = true;
    } else {
      log_opened = true;
    }
    if ((log_opened && create_new_db) || (log_opened && log_created)) {
      ib_logger(
        ib_stream,
        "Error: all log files must be"
        " created at the same time.\n"
        "All log files must be"
        " created also in database creation.\n"
        "If you want bigger or smaller"
        " log files, shut down the\n"
        "database and make sure there"
        " were no errors in shutdown.\n"
        "Then delete the existing log files."
        " Reconfigure InnoDB\n"
        "and start the database again.\n"
      );

      srv_startup_abort(DB_ERROR);
      return DB_ERROR;
    }
  }

  /* Open all log files and data files in the system tablespace: we
  keep them open until database shutdown */

  fil_open_log_and_system_tablespace_files();

  if (log_created && !create_new_db) {
    if (max_flushed_lsn != min_flushed_lsn) {
      ib_logger(
        ib_stream,
        "Cannot initialize created"
        " log files because\n"
        "data files were not in sync"
        " with each other\n"
        "or the data files are corrupt.\n"
      );

      srv_startup_abort(DB_ERROR);
      return DB_ERROR;
    }

    if (max_flushed_lsn < (uint64_t)1000) {
      ib_logger(
        ib_stream,
        "Cannot initialize created"
        " log files because\n"
        "data files are corrupt,"
        " or new data files were\n"
        "created when the database"
        " was started previous\n"
        "time but the database"
        " was not shut down\n"
        "normally after that.\n"
      );

      srv_startup_abort(DB_ERROR);
      return DB_ERROR;
    }

    mutex_enter(&(log_sys->mutex));

    recv_reset_logs(max_flushed_lsn, true);

    mutex_exit(&(log_sys->mutex));
  }

  trx_sys_file_format_init();

  if (create_new_db) {
    mtr_start(&mtr);
    fsp_header_init(0, sum_of_new_sizes, &mtr);

    mtr_commit(&mtr);

    trx_sys_create(srv_force_recovery);
    dict_create();
    srv_startup_is_before_trx_rollback_phase = false;
  } else {

    /* Check if we support the max format that is stamped
    on the system tablespace.
    Note:  We are NOT allowed to make any modifications to
    the TRX_SYS_PAGE_NO page before recovery  because this
    page also contains the max_trx_id etc. important system
    variables that are required for recovery.  We need to
    ensure that we return the system to a state where normal
    recovery is guaranteed to work. We do this by
    invalidating the buffer cache, this will force the
    reread of the page and restoration to its last known
    consistent state, this is REQUIRED for the recovery
    process to work. */
    err = trx_sys_file_format_max_check(srv_check_file_format_at_startup);

    if (err != DB_SUCCESS) {
      srv_startup_abort(err);
      return err;
    }

    /* We always try to do a recovery, even if the database had
    been shut down normally: this is the normal startup path */

    err =
      recv_recovery_from_checkpoint_start(srv_force_recovery, LOG_CHECKPOINT, IB_UINT64_T_MAX, min_flushed_lsn, max_flushed_lsn);

    if (err != DB_SUCCESS) {
      srv_startup_abort(err);
      return DB_ERROR;
    }

    /* Since the insert buffer init is in dict_boot, and the
    insert buffer is needed in any disk i/o, first we call
    dict_boot(). Note that trx_sys_init_at_db_start() only needs
    to access space 0, and the insert buffer at this stage already
    works for space 0. */

    dict_boot();
    trx_sys_init_at_db_start(srv_force_recovery);

    /* Initialize the fsp free limit global variable in the log
    system */
    fsp_header_get_free_limit();

    /* recv_recovery_from_checkpoint_finish needs trx lists which
    are initialized in trx_sys_init_at_db_start(). */

    recv_recovery_from_checkpoint_finish(srv_force_recovery);

    if (srv_force_recovery <= IB_RECOVERY_NO_TRX_UNDO) {
      /* In a crash recovery, we check that the info in data
      dictionary is consistent with what we already know
      about space id's from the call of
      fil_load_single_table_tablespaces().

      In a normal startup, we create the space objects for
      every table in the InnoDB data dictionary that has
      an .ibd file.

      We also determine the maximum tablespace id used. */

      dict_check_tablespaces_and_store_max_id(recv_needed_recovery);
    }

    srv_startup_is_before_trx_rollback_phase = false;
    recv_recovery_rollback_active();

    /* It is possible that file_format tag has never
    been set. In this case we initialize it to minimum
    value.  Important to note that we can do it ONLY after
    we have finished the recovery process so that the
    image of TRX_SYS_PAGE_NO is not stale. */
    trx_sys_file_format_tag_init();
  }

  if (!create_new_db && sum_of_new_sizes > 0) {
    /* New data file(s) were added */
    mtr_start(&mtr);

    fsp_header_inc_size(0, sum_of_new_sizes, &mtr);

    mtr_commit(&mtr);

    /* Immediately write the log record about increased tablespace
    size to disk, so that it is durable even if we crash
    quickly */

    log_buffer_flush_to_disk();
  }

  /* ib_logger(ib_stream, "Max allowed record size %lu\n",
  page_get_free_space_of_empty() / 2); */

  /* Create the thread which watches the timeouts for lock waits */
  os_thread_create(&srv_lock_timeout_thread, nullptr, thread_ids + 2 + SRV_MAX_N_IO_THREADS);

  /* Create the thread which warns of long semaphore waits */
  os_thread_create(&srv_error_monitor_thread, nullptr, thread_ids + 3 + SRV_MAX_N_IO_THREADS);

  /* Create the thread which prints InnoDB monitor info */
  os_thread_create(&srv_monitor_thread, nullptr, thread_ids + 4 + SRV_MAX_N_IO_THREADS);

  srv_is_being_started = false;

  if (trx_doublewrite == nullptr) {
    /* Create the doublewrite buffer to a new tablespace */

    err = trx_sys_create_doublewrite_buf();
  }

  if (err == DB_SUCCESS) {
    err = dict_create_or_check_foreign_constraint_tables();
  }

  if (err != DB_SUCCESS) {
    srv_startup_abort(err);
    return DB_ERROR;
  }

  /* Create the master thread which does purge and other utility
  operations */

  os_thread_create(&srv_master_thread, nullptr, thread_ids + (1 + SRV_MAX_N_IO_THREADS));
#ifdef UNIV_DEBUG
  /* buf_debug_prints = true; */
#endif /* UNIV_DEBUG */
  sum_of_data_file_sizes = 0;

  for (i = 0; i < srv_n_data_files; i++) {
    sum_of_data_file_sizes += srv_data_file_sizes[i];
  }

  tablespace_size_in_header = fsp_header_get_tablespace_size();

  if (!srv_auto_extend_last_data_file && sum_of_data_file_sizes != tablespace_size_in_header) {

    ib_logger(
      ib_stream,
      "Error: tablespace size"
      " stored in header is %lu pages, but\n"
      "the sum of data file sizes is %lu pages\n",
      (ulong)tablespace_size_in_header,
      (ulong)sum_of_data_file_sizes
    );

    if (srv_force_recovery == IB_RECOVERY_DEFAULT && sum_of_data_file_sizes < tablespace_size_in_header) {
      /* This is a fatal error, the tail of a tablespace is
      missing */

      ib_logger(
        ib_stream,
        "Cannot start InnoDB."
        " The tail of the system tablespace is\n"
        "missing. Have you set the"
        " data_file_path in an\n"
        "inappropriate way, removing"
        " ibdata files from there?\n"
        "You can set force_recovery=1"
        " to force\n"
        "a startup if you are trying"
        " to recover a badly corrupt database.\n"
      );

      srv_startup_abort(DB_ERROR);
      return DB_ERROR;
    }
  }

  if (srv_auto_extend_last_data_file && sum_of_data_file_sizes < tablespace_size_in_header) {

    ib_logger(
      ib_stream,
      "Error: tablespace size stored in header"
      " is %lu pages, but\n"
      "the sum of data file sizes"
      " is only %lu pages\n",
      (ulong)tablespace_size_in_header,
      (ulong)sum_of_data_file_sizes
    );

    if (srv_force_recovery == IB_RECOVERY_DEFAULT) {

      ib_logger(
        ib_stream,
        "Cannot start InnoDB. The tail of"
        " the system tablespace is\n"
        "missing. Have you set "
        " data_file_path in an\n"
        "inappropriate way, removing"
        " ibdata files from there?\n"
        "You can set force_recovery=1"
        " in to force\n"
        "a startup if you are trying to"
        " recover a badly corrupt database.\n"
      );

      srv_startup_abort(DB_ERROR);
      return DB_ERROR;
    }
  }

  /* Check that os_fast_mutexes work as expected */
  os_fast_mutex_init(&srv_os_test_mutex);

  if (0 != os_fast_mutex_trylock(&srv_os_test_mutex)) {
    ib_logger(
      ib_stream,
      "Error: pthread_mutex_trylock returns"
      " an unexpected value on\n"
      "success! Cannot continue.\n"
    );

    srv_startup_abort(DB_ERROR);
    return DB_ERROR;
  }

  os_fast_mutex_unlock(&srv_os_test_mutex);

  os_fast_mutex_lock(&srv_os_test_mutex);

  os_fast_mutex_unlock(&srv_os_test_mutex);

  os_fast_mutex_free(&srv_os_test_mutex);

  if (srv_print_verbose_log) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      " InnoDB %s started; "
      "log sequence number %lu\n",
      VERSION,
      srv_start_lsn
    );
  }

  if (srv_force_recovery != IB_RECOVERY_DEFAULT) {
    ib_logger(ib_stream, "!!! force_recovery is set to %lu !!!\n", (ulong)srv_force_recovery);
  }

  if (trx_doublewrite_must_reset_space_ids) {
    /* Actually, we did not change the undo log format between
    4.0 and 4.1.1, and we would not need to run purge to
    completion. Note also that the purge algorithm in 4.1.1
    can process the history list again even after a full
    purge, because our algorithm does not cut the end of the
    history list in all cases so that it would become empty
    after a full purge. That mean that we may purge 4.0 type
    undo log even after this phase.

    The insert buffer record format changed between 4.0 and
    4.1.1. It is essential that the insert buffer is emptied
    here! */

    ib_logger(
      ib_stream,
      "You are upgrading to an"
      " InnoDB version which allows multiple\n"
      "tablespaces. Wait that purge"
      " and insert buffer merge run to\n"
      "completion...\n"
    );
    for (;;) {
      os_thread_sleep(1000000);

      if (0 == strcmp(srv_main_thread_op_info, "waiting for server activity")) {

        break;
      }
    }
    ib_logger(
      ib_stream,
      "Full purge and insert buffer merge"
      " completed.\n"
    );

    trx_sys_mark_upgraded_to_multiple_tablespaces();

    ib_logger(
      ib_stream,
      "You have now successfully upgraded"
      " to the multiple tablespaces\n"
      "format. You should NOT DOWNGRADE"
      " to an earlier version of\n"
      "InnoDB! But if you absolutely need to"
      " downgrade, check\n"
      "the InnoDB website for details\n"
      "for instructions.\n"
    );
  }

  srv_file_per_table = srv_file_per_table_original_value;

  srv_was_started = true;

  return DB_SUCCESS;
}

/** Try to shutdown the InnoDB threads.
@return	true if all threads exited. */
static bool srv_threads_try_shutdown(os_event_t lock_timeout_thread_event) {
  /* Let the lock timeout thread exit */
  os_event_set(lock_timeout_thread_event);

  /* srv error monitor thread exits automatically, no need
  to do anything here */

  /* We wake the master thread so that it exits */
  srv_wake_master_thread();

  /* Exit the i/o threads */
  os_aio_wake_all_threads_at_shutdown();

  os_mutex_enter(os_sync_mutex);

  if (os_thread_count == 0) {
    /* All the threads have exited or are just exiting;
    NOTE that the threads may not have completed their
    exit yet. Should we use pthread_join() to make sure
    they have exited? Now we just sleep 0.1 seconds and
    hope that is enough! */

    os_mutex_exit(os_sync_mutex);

    os_thread_sleep(100000);

    return true;
  }

  os_mutex_exit(os_sync_mutex);

  os_thread_sleep(100000);

  return false;
}

/** All threads end up waiting for certain events. Put those events
to the signaled state. Then the threads will exit themselves in
os_thread_event_wait().
@return	true if all threads exited. */
static bool srv_threads_shutdown(void) {
  ulint i;

  srv_shutdown_state = SRV_SHUTDOWN_EXIT_THREADS;

  for (i = 0; i < 1000; i++) {

    if (srv_threads_try_shutdown(srv_lock_timeout_thread_event)) {

      return true;
    }
  }

  ib_logger(
    ib_stream,
    "Warning: %lu threads created by InnoDB"
    " had not exited at shutdown!\n",
    (ulong)os_thread_count
  );

  return false;
}

/** Shuts down the InnoDB database.
@return	DB_SUCCESS or error code */

enum db_err innobase_shutdown(ib_shutdown_t shutdown) /*!< in: shutdown flag */
{
  if (!srv_was_started) {
    if (srv_is_being_started) {
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Warning: shutting down"
        " a not properly started\n"
        "or created database!\n"
      );
    }

    ut_free_all_mem();

    return DB_SUCCESS;
  }

  /* This is currently required to inform the master thread only. Once
  we have contexts we can get rid of this global. */
  srv_fast_shutdown = shutdown;

  /* 1. Flush the buffer pool to disk, write the current lsn to
  the tablespace header(s), and copy all log data to archive.
  The step 1 is the real InnoDB shutdown. The remaining steps 2 - ...
  just free data structures after the shutdown. */

  if (shutdown == IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  User has requested a very fast shutdown"
      " without flushing "
      "the InnoDB buffer pool to data files."
      " At the next startup "
      "InnoDB will do a crash recovery!\n"
    );
  }

  logs_empty_and_mark_files_at_shutdown(srv_force_recovery, shutdown);

  /* In a 'very fast' shutdown, we do not need to wait for these threads
  to die; all which counts is that we flushed the log; a 'very fast'
  shutdown is essentially a crash. */

  if (shutdown == IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
    return DB_SUCCESS;
  }

  srv_threads_shutdown();

  log_shutdown();
  lock_sys_close();
  thr_local_close();
  trx_sys_file_format_close();
  trx_sys_close();
  dict_close(); /* Must be called before buf_close(). */
  buf_close();
  fil_close();
  os_aio_close();
  srv_free();

  /* 3. Free all InnoDB's own mutexes and the os_fast_mutexes inside
  them */
  sync_close();

  /* 4. Free the os_conc_mutex and all os_events and os_mutexes */
  os_sync_free();

  /* 5. Free all allocated memory */
  pars_close();

  log_mem_free();
  buf_mem_free();

  /* This variable should come from the user and should not be
  malloced by InnoDB. */
  srv_data_home = nullptr;

  /* This variable should come from the user and should not be
  malloced by InnoDB. */

  pars_lexer_var_init();

  ut_free_all_mem();

  if (os_thread_count != 0 || os_event_count != 0 || os_mutex_count != 0 || os_fast_mutex_count != 0) {
    ib_logger(
      ib_stream,
      "Warning: some resources were not"
      " cleaned up in shutdown:\n"
      "threads %lu, events %lu,"
      " os_mutexes %lu, os_fast_mutexes %lu\n",
      (ulong)os_thread_count,
      (ulong)os_event_count,
      (ulong)os_mutex_count,
      (ulong)os_fast_mutex_count
    );
  }

  if (lock_latest_err_stream) {
    fclose(lock_latest_err_stream);
  }

  if (srv_print_verbose_log) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Shutdown completed;"
      " log sequence number %lu\n",
      srv_shutdown_lsn
    );
  }

  srv_was_started = false;
  srv_start_has_been_called = false;

  srv_modules_var_init();
  srv_var_init();

  srv_data_file_names = nullptr;
  srv_log_group_home_dirs = nullptr;

  return DB_SUCCESS;
}
