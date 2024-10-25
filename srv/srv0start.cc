/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, Google Inc.
Copyright (c) 2009, Percona Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

#include "btr0btr.h"
#include "btr0cur.h"
#include "btr0pcur.h"
#include "buf0buf.h"
#include "buf0dblwr.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "data0data.h"
#include "data0type.h"
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
#include "trx0purge.h"
#include "trx0roll.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "srv0srv.h"
#include "usr0sess.h"
#include "ut0mem.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include <filesystem>

/** System tablespace initial size.  */
constexpr ulint SYSTEM_IBD_FILE_INITIAL_SIZE = 32 * 1024 * 1024;

/* Log sequence number immediately after startup */
uint64_t srv_start_lsn;

/** Log sequence number at shutdown */
uint64_t srv_shutdown_lsn;

/** true if a raw partition is in use */
bool srv_start_raw_disk_in_use = false;

/** true if the server is being started, before rolling back any
incomplete transactions */
bool srv_startup_is_before_trx_rollback_phase = false;

/** true if the server is being started */
bool srv_is_being_started = false;

/** true if the server was successfully started */
bool srv_was_started = false;

/** At a shutdown this value climbs from SRV_SHUTDOWN_NONE to
SRV_SHUTDOWN_CLEANUP and then to SRV_SHUTDOWN_LAST_PHASE, and so on */
enum srv_shutdown_state srv_shutdown_state = SRV_SHUTDOWN_NONE;

/** io_handler_thread parameters for thread identification */
static std::array<ulint, SRV_MAX_N_IO_THREADS + 6> n;

/** io_handler_thread identifiers */
static os_thread_id_t thread_ids[SRV_MAX_N_IO_THREADS + 6];


/** The system data file name. */
static std::string srv_system_file_name{};

/** All threads end up waiting for certain events. Put those events
to the signaled state. Then the threads will exit themselves in
os_thread_event_wait().
@return	true if all threads exited. */
static bool srv_threads_shutdown() noexcept;

/** FIXME: Make this configurable. */
constexpr ulint OS_AIO_N_PENDING_IOS_PER_THREAD = 256;

/**
 * @brief Empties the logs and marks files at shutdown.
 *
 * @param recovery The recovery flag.
 * @param shutdown The shutdown flag.
 */
static void srv_prepare_for_shutdown(ib_recovery_t recovery, ib_shutdown_t shutdown) noexcept;


void *io_handler_thread(void *arg) {
  auto segment = *((ulint *)arg);

  while (srv_fil->aio_wait(segment)) { }

  /* We count the number of threads in os_thread_exit(). A created
  thread should always use that to exit and not use return() to exit.
  The thread actually never comes here because it is exited in an
  os_event_wait(). */

  os_thread_exit();

  return nullptr;
}

/**
 * @brief Creates a log file in the log group.
 *
 * @param[in] filename          Log file name to create
 *
 * @return DB_SUCCESS or error code
 */
static db_err create_log_file(const std::string &filename) noexcept {
  ut_a(!filename.empty());

  bool success{};
  auto fh = os_file_create(filename.c_str(), OS_FILE_CREATE, OS_FILE_NORMAL, OS_LOG_FILE, &success);

  if (!success) {
    return os_file_get_last_error(false) != OS_FILE_ALREADY_EXISTS ? DB_ERROR : DB_TABLE_EXISTS;
  }

  log_warn(std::format("Log file {} did not exist: creating a new log file", filename));

  log_info(std::format(
    "Setting log file {} size to {} MB",
    filename, srv_config.m_log_file_curr_size / (1024 * 1024)));

  log_info("Database physically writes the file full: wait...");

  success = os_file_set_size(filename.c_str(), fh, srv_config.m_log_file_curr_size);

  {
    auto success = os_file_close(fh);
    ut_a(success);
  }

  if (!success) {
    log_err(std::format("Can't create {}: probably out of disk space", filename));
    return DB_ERROR;
  } else {
    srv_fil->node_create(filename.c_str(), srv_config.m_log_file_size, SRV_LOG_SPACE_FIRST_ID, false);
    return DB_SUCCESS;
  }

}

/**
 * @brief Opens a log file.
 *
 * @param filename              Log file name to open
 *
 * @return DB_SUCCESS or error code
 */
static db_err open_log_file(const std::string &filename) noexcept {
  ut_a(!filename.empty());

  bool success{};
  auto fh = os_file_create(filename.c_str(), OS_FILE_OPEN, OS_FILE_AIO, OS_LOG_FILE, &success);

  if (!success) {
    log_err("Cannot open log file ", filename);

    return DB_ERROR;
  }

  off_t size{};
  success = os_file_get_size(fh, &size);
  ut_a(success);

  if (size != off_t(srv_config.m_log_file_curr_size)) {

    log_err(std::format(
      "Log file {} is of different size {} bytes than the configured {} bytes!",
      filename, size, srv_config.m_log_file_curr_size
    ));

    return DB_ERROR;
  }

  success = os_file_close(fh);
  ut_a(success);

  srv_fil->node_create(filename.c_str(), srv_config.m_log_file_size, SRV_LOG_SPACE_FIRST_ID, false);

  return DB_SUCCESS;
}

/**
 * @brief Creates the system tablespace.
 * 
 * @return DB_SUCCESS or error code
 */
static db_err create_system_tablespace(const std::string &path) noexcept {
  namespace fs = std::filesystem;

  ut_a(!path.empty());

  fs::path filename(path);

  {
    auto home_dir = filename.parent_path();

    if (fs::status(home_dir).type() != fs::file_type::directory) {
      log_err(std::format(
        "Data home directory '{}' does not exist, is not a directory"
        " or you don't have read-write permissions. We require that"
        " the home directory must exist and be readable and writeable",
        home_dir.generic_string()));
      return DB_ERROR;
    }
  }

  if (fs::exists(filename) && fs::status(filename).type() == fs::file_type::regular) {
    log_warn(std::format("System tablespace '{}' already exists", filename.generic_string()));
    return DB_TABLE_EXISTS;
  }

  log_info(std::format(
    "Creating the system tablespace: 'system.idb' of size {} MB ",
    SYSTEM_IBD_FILE_INITIAL_SIZE/(1024 * 1024)
  ));

  bool success{};
  auto fh = os_file_create(filename.c_str(), OS_FILE_CREATE, OS_FILE_NORMAL, OS_DATA_FILE, &success);

  if (!success && os_file_get_last_error(false) != OS_FILE_ALREADY_EXISTS) {
    log_err(std::format("Can't create {}", filename.generic_string()));
    return DB_ERROR;
  }

  success = os_file_set_size(filename.c_str(), fh, SYSTEM_IBD_FILE_INITIAL_SIZE);
  ut_a(success);

  success = os_file_close(fh);
  ut_a(success);

  return DB_SUCCESS;
}

/**
 * @brief Opens the system tablespce, it must exist and be readable and writeable.
 * 
 * @param[in] path Path to the system tablespace
 * @param[out] flushed_lsn  Max of flushed lsn values in data files
 * 
 * @return DB_SUCCESS or error code
 */
static db_err open_system_tablespace(const std::string &path, lsn_t &flushed_lsn) noexcept {
  namespace fs = std::filesystem;

  ut_a(!path.empty());

  fs::path filename(path);

  /* Must exist if we've been asked to open the file. */
  ut_a(fs::status(filename).type() == fs::file_type::regular);

  bool success{};
  auto fh = os_file_create(filename.c_str(), OS_FILE_OPEN, OS_FILE_NORMAL, OS_DATA_FILE, &success);

  if (!success && os_file_get_last_error(false) != OS_FILE_ALREADY_EXISTS) {
    log_err(std::format("Can't open {}", filename.generic_string()));
    return DB_ERROR;
  }

  off_t size{};

  {
    auto ret = os_file_get_size(fh, &size);
    ut_a(ret);
  }

  /* Convert to number of pages. */
  size /= UNIV_PAGE_SIZE;

  srv_fil->read_flushed_lsn(fh, flushed_lsn);

  {
    auto success = os_file_close(fh);
    ut_a(success);
  }

  {
    auto success = srv_fil->space_create(filename.c_str(), SYS_TABLESPACE, 0, FIL_TABLESPACE);
    ut_a(success);
    ut_a(srv_fil->validate());
  }

  srv_fil->node_create(filename.c_str(), size, 0, false);

  return DB_SUCCESS;
}

/**
 * @brief Abort the startup process and shutdown the minimum set of
 * sub-systems required to create files and.
 * 
 * @param err Current error code
 */
static void srv_startup_abort(db_err err) noexcept {
  /* This is currently required to inform the master thread only. Once
  we have contexts we can get rid of this global. */
  srv_config.m_fast_shutdown = IB_SHUTDOWN_NORMAL;

  /* For fatal errors we want to avoid writing to the data files. */
  if (err != DB_FATAL) {

    /* Only if the redo log systemhas been initialized. */
    if (log_sys != nullptr && UT_LIST_GET_LEN(log_sys->m_log_groups) > 0) {
      srv_prepare_for_shutdown(srv_config.m_force_recovery, srv_config.m_fast_shutdown);
    }

    srv_fil->close_all_files();
  }

  srv_threads_shutdown();

  log_sys->shutdown();

  srv_buf_pool->close();

  srv_aio->shutdown();

  delete srv_fil;
  srv_fil = nullptr;

  os_file_free();

  Log::destroy(log_sys);

  AIO::destroy(srv_aio);

  delete srv_buf_pool;
}

ib_err_t InnoDB::start() noexcept {
  ut_a(!srv_was_started);

  // FIXME:
  ib_stream = stderr;

  static_assert(sizeof(ulint) == sizeof(uintptr_t));

  /* System tables are created in tablespace 0. Thus, we must
  temporarily clear m_config.m_file_per_table.  This is ok,
  because the server will not accept connections (which could
  modify m_config.m_file_per_table) until this function has returned. */
  srv_config.m_file_per_table = false;

  IF_DEBUG(log_warn("!!!!!!!! UNIV_DEBUG switched on !!!!!!!!!");)

  IF_SYNC_DEBUG(log_warn("!!!!!!!! UNIV_SYNC_DEBUG switched on !!!!!!!!!");)

  if (likely(srv_config.m_use_sys_malloc)) {
    log_warn("The InnoDB memory heap is disabled");
  }

  ut_d(log_do_write = true);

  /*	yydebug = true; */

  srv_is_being_started = true;

  srv_startup_is_before_trx_rollback_phase = true;

  /* Note that the call srv_boot() also changes the values of
  some variables to the units used by InnoDB internally */

  /* Set the maximum number of threads which can wait for a semaphore
  inside this is the 'sync wait array' size, as well as the
  maximum number of threads that can wait in the 'srv_conc array' for
  their time to enter InnoDB. */

  if (srv_config.m_buf_pool_size >= 1000 * 1024 * 1024) {
    /* If buffer pool is less than 1000 MB, assume fewer threads. */
    srv_config.m_max_n_threads = 8192;
  } else if (srv_config.m_buf_pool_size >= 8 * 1024 * 1024) {
    srv_config.m_max_n_threads = 4096;
  } else {
    srv_config.m_max_n_threads = 128;
  }

  auto err = InnoDB::boot();

  if (err != DB_SUCCESS) {

    return err;
  }

  /* file_io_threads used to be user settable, now it's just a
     sum of read_io_threads and write_io_threads */
  const auto n_file_io_threads = 1 + srv_config.m_n_read_io_threads + srv_config.m_n_write_io_threads;

  ut_a(n_file_io_threads <= SRV_MAX_N_IO_THREADS);

#ifdef UNIV_DEBUG
  /* We have observed deadlocks with a 5MB buffer pool but
  the actual lower limit could very well be a little higher. */

  if (srv_config.m_buf_pool_size <= 5 * 1024 * 1024) {

    log_warn(std::format(
      "Small buffer pool size ({}M), the flst_validate() debug function "
      "can cause a deadlock if the buffer pool fills up.",
      srv_config.m_buf_pool_size / 1024 / 1024
    ));
  }
#endif /* UNIV_DEBUG */

  if (srv_config.m_n_log_files * srv_config.m_log_file_size >= 262144) {
    log_err("Combined size of log files must be < 4 GB");
    return DB_ERROR;
  }

  const auto io_limit = OS_AIO_N_PENDING_IOS_PER_THREAD;

  os_file_init();

  srv_aio = AIO::create(io_limit, srv_config.m_n_read_io_threads, srv_config.m_n_write_io_threads);

  if (srv_aio == nullptr) {
    log_err("Failed to create an AIO instance.");
    os_file_free();
    return DB_OUT_OF_MEMORY;
  }

  ut_a(srv_fil == nullptr);

  srv_fil = new (std::nothrow) Fil(srv_config.m_max_n_open_files);

  if (srv_fil == nullptr) {
    AIO::destroy(srv_aio);

    os_file_free();

    log_err("Fatal error: cannot allocate the memory for the file system");

    return DB_OUT_OF_MEMORY;
  }

  srv_buf_pool = new (std::nothrow) Buf_pool();

  if (!srv_buf_pool->open(srv_config.m_buf_pool_size)) {
    /* Shutdown all sub-systems that have been initialized. */
    delete srv_fil;
    srv_fil = nullptr;

    AIO::destroy(srv_aio);

    os_file_free();

    log_err("Fatal error: cannot allocate the memory for the buffer pool");

    return DB_OUT_OF_MEMORY;
  }

  ut_a(log_sys == nullptr);
  log_sys = Log::create();

  ut_a(srv_fsp == nullptr);
  srv_fsp = FSP::create(log_sys, srv_fil, srv_buf_pool);

  ut_a(srv_trx_sys == nullptr);
  srv_trx_sys = Trx_sys::create(srv_fsp);

  ut_a(srv_lock_sys == nullptr);
  srv_lock_sys = Lock_sys::create(srv_trx_sys, srv_config.m_lock_table_size);

  ut_a(srv_btree_sys == nullptr);
  srv_btree_sys = Btree::create(srv_lock_sys, srv_fsp, srv_buf_pool);

  ut_a(srv_undo == nullptr);
  srv_undo = Undo::create(srv_fsp);

  /* Create i/o-handler threads: */

  for (ulint i{}; i < n_file_io_threads; ++i) {
    n[i] = i;

    os_thread_create(io_handler_thread, &n[i], thread_ids + i);
  }

  bool create_new_db{};
  lsn_t max_flushed_lsn{};

  {
    namespace fs = std::filesystem;

    fs::path home(srv_config.m_data_home);

    if (home.empty()) {
      log_err("Data home directory is not set");
      return DB_ERROR;
    }

    if (!fs::exists(home) || fs::status(home).type() != fs::file_type::directory) {
      log_err("Data home directory does not exist or is not a directory");
      return DB_ERROR;
    }

    fs::path filename(home);

    filename /= "system.ibd";

    if (!fs::exists(filename)) {
      err = create_system_tablespace(filename);

      if (err == DB_SUCCESS) {
        create_new_db = true;
      } else if (err != DB_TABLE_EXISTS) {
        srv_startup_abort(err);
        return DB_ERROR;
      }
    }

    err = open_system_tablespace(filename, max_flushed_lsn);

    if (err != DB_SUCCESS) {
      srv_startup_abort(err);
      return DB_ERROR;
    }
  }

  bool log_opened{};
  bool log_created{};

  /* Note: Currently we support a single log group (0). */
  std::string log_dir{InnoDB::get_log_dir()};

  ut_a(srv_config.m_n_log_files >= 2);

  auto success = srv_fil->space_create(log_dir.c_str(), SRV_LOG_SPACE_FIRST_ID, 0, FIL_LOG);
  ut_a(success);
  ut_a(srv_fil->validate());

  for (ulint i{}; i < srv_config.m_n_log_files; ++i) {
    namespace fs = std::filesystem;

    fs::path filename(fs::path(log_dir) / std::format("ib_logfile{}", i));

    if (fs::exists(filename) && fs::status(filename).type() == fs::file_type::regular) {
      err = open_log_file(filename.generic_string());
      log_opened = true;
    } else {
      err = create_log_file(filename.generic_string());
      log_created = true;
    }

    if (err != DB_SUCCESS) {
      srv_startup_abort(err);
      return err;
    }

    if (i == 0) {
      log_sys->group_init(i, srv_config.m_n_log_files, srv_config.m_log_file_size * UNIV_PAGE_SIZE, SRV_LOG_SPACE_FIRST_ID);
    }

    if ((log_opened && create_new_db) || (log_opened && log_created)) {
      log_err(
        "All log files must be created at the same time. All log files must be"
        " created also in database creation. If you want bigger or smaller"
        " log files, shut down the database and make sure there were no errors"
        " in shutdown. Then delete the existing log files. Reconfigure InnoDB"
        " and start the database again."
      );

      srv_startup_abort(DB_ERROR);
      return DB_ERROR;
    }
  }

  /* Open all log files and the system tablespace: we keep them open
  until database shutdown */

  srv_fil->open_log_and_system_tablespace();

  if (log_created && !create_new_db) {
    if (max_flushed_lsn < lsn_t(1000)) {
      log_err(
        "Cannot initialize created log files because data files are corrupt,"
        " or new data files were created when the database was started previous"
        " time but the database was not shut down normally after that."
      );

      srv_startup_abort(DB_ERROR);
      return DB_ERROR;
    }

    log_sys->acquire();;

    recv_reset_logs(max_flushed_lsn, true);

    log_sys->release();
  }

  if (create_new_db) {
    mtr_t mtr;

    mtr.start();

    srv_fsp->header_init(SYS_TABLESPACE, SYSTEM_IBD_FILE_INITIAL_SIZE / UNIV_PAGE_SIZE, &mtr);

    mtr.commit();

    err = srv_trx_sys->create_system_tablespace();

    if (err == DB_SUCCESS) {
      err = srv_trx_sys->start(srv_config.m_force_recovery);
    }

    if (err != DB_SUCCESS) {
      srv_startup_abort(err);
      return DB_ERROR;
    }

    ut_a(srv_dict_sys == nullptr);
    srv_dict_sys = Dict::create(srv_btree_sys);

    if (srv_dict_sys == nullptr) {
      srv_startup_abort(DB_OUT_OF_MEMORY);
      return DB_ERROR;
    }

    {
      // FIXME: These should be created in a separate DML related function
      ut_a(srv_row_upd == nullptr);
      srv_row_upd = Row_update::create(srv_dict_sys, srv_lock_sys);

      if (srv_row_upd == nullptr) {
        srv_startup_abort(DB_OUT_OF_MEMORY);
        return DB_ERROR;
      }

      ut_a(srv_row_ins == nullptr);
      srv_row_ins = Row_insert::create(srv_dict_sys, srv_lock_sys, srv_row_upd);

      if (srv_row_ins == nullptr) {
        srv_startup_abort(DB_OUT_OF_MEMORY);
        return DB_ERROR;
      }

      ut_a(srv_row_sel == nullptr);
      srv_row_sel = Row_sel::create(srv_dict_sys, srv_lock_sys);

      if (srv_row_sel == nullptr) {
        srv_startup_abort(DB_OUT_OF_MEMORY);
        return DB_ERROR;
      }
    }

    if (auto err = srv_dict_sys->create_instance(); err != DB_SUCCESS) {
      srv_startup_abort(err);
      return DB_ERROR;
    }

    if (auto err = srv_dict_sys->open(false); err != DB_SUCCESS) {
      srv_startup_abort(err);
      return DB_ERROR;
    }

    srv_startup_is_before_trx_rollback_phase = false;

    /* Create the doublewrite buffer for the system tablespace */
    {
      ut_a(srv_dblwr == nullptr);
      srv_dblwr = DBLWR::create(srv_fsp);

      if (srv_dblwr == nullptr) {
        srv_startup_abort(DB_OUT_OF_MEMORY);
        return DB_ERROR;
      }

      err = srv_dblwr->initialize();

      if (err != DB_SUCCESS) {
        srv_startup_abort(err);
        return DB_ERROR;
      }

      /* Flush the modified pages to disk and make a checkpoint */
      log_sys->make_checkpoint_at(IB_UINT64_T_MAX, true);
    }

  } else {

    if (err != DB_SUCCESS) {
      srv_startup_abort(err);
      return DB_ERROR;
    }

    /* Setup the doublewrite buffer, open and restore. */
    ut_a(srv_dblwr == nullptr);
    srv_dblwr = DBLWR::create(srv_fsp);

    if (srv_dblwr == nullptr) {
      srv_startup_abort(DB_ERROR);
      return DB_ERROR;
    }

    std::pair<page_no_t, page_no_t> offsets{};

    if (!DBLWR::check_if_exists(srv_fil, offsets)) {
      err = srv_dblwr->initialize();

      if (err != DB_SUCCESS) {
        srv_startup_abort(err);
        return DB_ERROR;
      }
    } else {
      srv_dblwr->m_block1 = offsets.first;
      srv_dblwr->m_block2 = offsets.second;
    }

    log_warn("Reading tablespace information from the .ibd files...");

    /* Recursively scan to a depth of 2. InnoDB needs to do this because the DD
    can't be accessed until recovery is done. So we have this simplistic scheme. */
    srv_fil->load_single_table_tablespaces(srv_config.m_data_home, srv_config.m_force_recovery, 2);

    /* We always instantiate the DBLWR buffer. Restore the pages in data files,
     * and restore them from the doublewrite buffer if possible */
    if (srv_config.m_force_recovery < IB_RECOVERY_NO_LOG_REDO) {
      log_warn("Restoring possible half-written data pages from the doublewrite buffer...");
      srv_dblwr->recover_pages();
    }

    /* We always try to do a recovery, even if the database had
    been shut down normally: this is the normal startup path */

    err = recv_recovery_from_checkpoint_start(srv_dblwr, srv_config.m_force_recovery, max_flushed_lsn);

    if (err != DB_SUCCESS) {
      srv_startup_abort(err);
      return DB_ERROR;
    }

    err = srv_trx_sys->start(srv_config.m_force_recovery);

    {
      auto free_limit = srv_fsp->init_system_space_free_limit();
      log_info(std::format("Free limit of system.ibd set to {} MB", free_limit));
    }

    srv_startup_is_before_trx_rollback_phase = false;

    /* recv_recovery_from_checkpoint_finish needs trx lists which
    are initialized in trx_sys_init_at_db_start(). */

    recv_recovery_from_checkpoint_finish(srv_dblwr, srv_config.m_force_recovery);

    ut_a(srv_dict_sys == nullptr);

    srv_dict_sys = Dict::create(srv_btree_sys);

    if (srv_dict_sys == nullptr) {
      srv_startup_abort(DB_OUT_OF_MEMORY);
      return DB_ERROR;
    }

    {
      ut_a(srv_row_upd == nullptr);
      srv_row_upd = Row_update::create(srv_dict_sys, srv_lock_sys);

      if (srv_row_upd == nullptr) {
        srv_startup_abort(DB_OUT_OF_MEMORY);
        return DB_ERROR;
      }

      ut_a(srv_row_ins == nullptr);
      srv_row_ins = Row_insert::create(srv_dict_sys, srv_lock_sys, srv_row_upd);

      if (srv_row_ins == nullptr) {
        srv_startup_abort(DB_OUT_OF_MEMORY);
        return DB_ERROR;
      }

      ut_a(srv_row_sel == nullptr);
      srv_row_sel = Row_sel::create(srv_dict_sys, srv_lock_sys);

      if (srv_row_sel == nullptr) {
        srv_startup_abort(DB_OUT_OF_MEMORY);
        return DB_ERROR;
      }
    }

    if (srv_config.m_force_recovery <= IB_RECOVERY_NO_TRX_UNDO) {
      /* In a crash recovery, we check that the info in data
      dictionary is consistent with what we already know
      about space id's from the call of
      Fil::load_single_table_tablespaces().

      In a normal startup, we create the space objects for
      every table in the InnoDB data dictionary that has
      an .ibd file.

      We also determine the maximum tablespace id used. */

      if (auto err = srv_dict_sys->open(recv_needed_recovery); err != DB_SUCCESS) {
        srv_startup_abort(err);
        return DB_ERROR;
      }
    }

    srv_startup_is_before_trx_rollback_phase = false;

    recv_recovery_rollback_active();
  }

  log_info("Max allowed record size ", page_get_free_space_of_empty() / 2);

  /* Create the thread which watches the timeouts for lock waits */
  os_thread_create(&InnoDB::lock_timeout_thread, nullptr, &thread_ids[2 + SRV_MAX_N_IO_THREADS]);

  /* Create the thread which warns of long semaphore waits */
  os_thread_create(&InnoDB::error_monitor_thread, nullptr, &thread_ids[3 + SRV_MAX_N_IO_THREADS]);

  /* Create the thread which prints InnoDB monitor info */
  os_thread_create(&InnoDB::monitor_thread, nullptr, &thread_ids[4 + SRV_MAX_N_IO_THREADS]);

  srv_is_being_started = false;

  ut_a(err == DB_SUCCESS);
  err = srv_dict_sys->m_store.create_or_check_foreign_constraint_tables();

  if (err != DB_SUCCESS) {
    srv_startup_abort(err);
    return DB_ERROR;
  }

  /* Create the master thread which does purge and other utility
  operations */

  os_thread_create(&InnoDB::master_thread, nullptr, thread_ids + (1 + SRV_MAX_N_IO_THREADS));

  {
    const auto size = srv_fsp->get_system_space_size();
    log_info(std::format("system.ibd file size in the header is {} pages", size));
  }

  log_info(std::format(
    "InnoDB {} started; log sequence number {}",
    VERSION, srv_start_lsn
  ));

  if (srv_config.m_force_recovery != IB_RECOVERY_DEFAULT) {
    log_warn(std::format("!!! force_recovery is set to {} !!!", (int) srv_config.m_force_recovery));
  }

  srv_was_started = true;

  return DB_SUCCESS;
}

/**
 * Try to shutdown the InnoDB threads.
 * 
 * @return	true if all threads exited.
 */
static bool srv_threads_try_shutdown(Cond_var* lock_timeout_thread_event) noexcept  {
  /* Let the lock timeout thread exit */
  os_event_set(lock_timeout_thread_event);

  /* srv error monitor thread exits automatically, no need
  to do anything here */

  /* We wake the master thread so that it exits */
  InnoDB::wake_master_thread();

  srv_aio->shutdown();

  if (os_thread_count.load(std::memory_order_relaxed) == 0) {
    /* All the threads have exited or are just exiting;
    NOTE that the threads may not have completed their
    exit yet. */
    return true;
  } else {
    return false;
  }
}

/**
 * All threads end up waiting for certain events. Put those events
 * to the signaled state. Then the threads will exit themselves in
 * os_thread_event_wait().
 * 
 * @return	true if all threads exited.
 */
static bool srv_threads_shutdown() noexcept {
  srv_shutdown_state = SRV_SHUTDOWN_EXIT_THREADS;

  for (ulint i{}; i < 1000; ++i) {

    if (srv_threads_try_shutdown(srv_lock_timeout_thread_event)) {

      return true;
    }
  }

  log_warn(std::format(
    "{} threads created by InnoDB had not exited at shutdown!",
    (ulong)os_thread_count.load(std::memory_order_relaxed)
  ));

  return false;
}

static void srv_prepare_for_shutdown(ib_recovery_t recovery, ib_shutdown_t shutdown) noexcept {
  ut_a(log_sys != nullptr);
  ut_a(UT_LIST_GET_LEN(log_sys->m_log_groups) > 0);

  if (srv_print_verbose_log) {
    log_info("Starting shutdown...");
  }

  /* Wait until the master thread and all other operations are idle: our
  algorithm only works if the server is idle at shutdown */

  srv_shutdown_state = SRV_SHUTDOWN_CLEANUP;

  lsn_t lsn;

  for (;;) {
    os_thread_sleep(100000);

    mutex_enter(&kernel_mutex);

    /* We need the monitor threads to stop before we proceed with a
    normal shutdown. In case of very fast shutdown, however, we can
    proceed without waiting for monitor threads. */

    if (shutdown != IB_SHUTDOWN_NO_BUFPOOL_FLUSH &&
        (srv_error_monitor_active || srv_lock_timeout_active || srv_monitor_active)) {

      mutex_exit(&kernel_mutex);
      continue;
    }

    /* Check that there are no longer transactions. We need this wait even
    for the 'very fast' shutdown, because the InnoDB layer may have
    committed or prepared transactions and we don't want to lose them. */

    if (srv_trx_sys != nullptr && (srv_trx_sys->m_n_user_trx > 0 || !srv_trx_sys->m_trx_list.empty())) {

      mutex_exit(&kernel_mutex);

      continue;
    }

    if (shutdown == IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
      /* In this fastest shutdown we do not flush the buffer pool:
      it is essentially a 'crash' of the InnoDB server. Make sure
      that the log is all flushed to disk, so that we can recover
      all committed transactions in a crash recovery. We must not
      write the lsn stamps to the data files, since at a startup
      InnoDB deduces from the stamps if the previous shutdown was
      clean. */

      log_sys->buffer_flush_to_disk();

      mutex_exit(&kernel_mutex);

      return; /* We SKIP ALL THE REST !! */
    }

    /* Check that the master thread is suspended */
    if (srv_n_threads_active[SRV_MASTER] != 0) {

      mutex_exit(&kernel_mutex);

      continue;
    }

    mutex_exit(&kernel_mutex);

    log_sys->acquire();

    if (log_sys->m_n_pending_checkpoint_writes || log_sys->m_n_pending_writes) {

      log_sys->release();

      continue;
    }

    log_sys->release();

    if (srv_buf_pool->is_io_pending()) {

      continue;
    }

    log_sys->make_checkpoint_at(IB_UINT64_T_MAX, true);

    log_sys->acquire();

    lsn = log_sys->m_lsn;

    if (lsn != log_sys->m_last_checkpoint_lsn) {

      log_sys->release();

      continue;
    }

    log_sys->release();

    mutex_enter(&kernel_mutex);

    /* Check that the master thread has stayed suspended */
    if (srv_n_threads_active[SRV_MASTER] != 0) {
      log_warn("The master thread woke up during shutdown");

      mutex_exit(&kernel_mutex);

      continue;
    }

    mutex_exit(&kernel_mutex);

    srv_fil->flush_file_spaces(FIL_TABLESPACE);
    srv_fil->flush_file_spaces(FIL_LOG);

    /* The call srv_fil->write_flushed_lsn_to_data_files() will pass the buffer
    pool: therefore it is essential that the buffer pool has been
    completely flushed to disk! (We do not call srv_fil->write... if the
    'very fast' shutdown is enabled.) */

    if (srv_buf_pool->all_freed()) {
      break;
    }
  }

  srv_shutdown_state = SRV_SHUTDOWN_LAST_PHASE;

  /* Make some checks that the server really is quiet */
  ut_a(lsn == log_sys->m_lsn);
  ut_a(srv_buf_pool->all_freed());
  ut_a(srv_n_threads_active[SRV_MASTER] == 0);

  if (lsn < srv_start_lsn) {
    log_err(std::format(
      "Log sequence number at shutdown {} is lower than at startup {}",
      lsn, srv_start_lsn
    ));
  }

  srv_shutdown_lsn = lsn;

  srv_fil->write_flushed_lsn_to_data_files(lsn);

  srv_fil->flush_file_spaces(FIL_TABLESPACE);

  srv_fil->close_all_files();

  /* Make some checks that the server really is quiet */
  ut_a(srv_n_threads_active[SRV_MASTER] == 0);
  ut_a(srv_buf_pool->all_freed());
  ut_a(lsn == log_sys->m_lsn);
}

db_err InnoDB::shutdown(ib_shutdown_t shutdown) noexcept {
  ut_a(srv_was_started);

  /* This is currently required to inform the master thread only. Once
  we have contexts we can get rid of this global. */
  srv_config.m_fast_shutdown = shutdown;

  /* 1. Flush the buffer pool to disk, write the current lsn to
  the tablespace header(s), and copy all log data to archive.
  The step 1 is the real InnoDB shutdown. The remaining steps 2 - ...
  just free data structures after the shutdown. */

  if (shutdown == IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
    log_warn(
      "User has requested a very fast shutdown without flushing"
      " the InnoDB buffer pool to data files. At the next startup"
      " InnoDB will do a crash recovery!"
    );
  }

  /* Only if the redo log systemhas been initialized. */
  if (log_sys != nullptr && UT_LIST_GET_LEN(log_sys->m_log_groups) > 0) {
    srv_prepare_for_shutdown(srv_config.m_force_recovery, shutdown);
  }

  /* In a 'very fast' shutdown, we do not need to wait for these threads
  to die; all which counts is that we flushed the log; a 'very fast'
  shutdown is essentially a crash. */

  if (shutdown == IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
    return DB_SUCCESS;
  }

  srv_threads_shutdown();

  log_sys->shutdown();

  Row_insert::destroy(srv_row_ins);

  Row_update::destroy(srv_row_upd);

  Row_sel::destroy(srv_row_sel);

  /* Must be called before Buf_pool::close(). */
  Dict::destroy(srv_dict_sys);

  Btree::destroy(srv_btree_sys);

  Lock_sys::destroy(srv_lock_sys);

  Trx_sys::destroy(srv_trx_sys);

  Undo::destroy(srv_undo);

  srv_buf_pool->close();

  FSP::destroy(srv_fsp);

  srv_aio->shutdown();

  delete srv_fil;
  srv_fil = nullptr;

  InnoDB::free();

  AIO::destroy(srv_aio);

  /* 3. Free all InnoDB's own mutexes and the os_fast_mutexes inside them */
  sync_close();

  /* 4. Free the os_conc_mutex and all os_events and os_mutexes */
  os_sync_free();

  /* 5. Free all allocated memory */
  pars_close();

  Log::destroy(log_sys);

  delete srv_buf_pool;
  srv_buf_pool = nullptr;

  /* This variable should come from the user and should not be
  malloced by InnoDB. */
  srv_config.m_data_home = nullptr;

  /* This variable should come from the user and should not be
  malloced by InnoDB. */

  pars_lexer_var_init();

  ut_delete_all_mem();

  if (os_thread_count != 0 || Cond_var::s_count != 0 || os_mutex_count() != 0) {
    log_warn(std::format(
      "Some resources were not cleaned up in shutdown:"
      " threads {}, events {}, os_mutexes {}",
      os_thread_count.load(),
      Cond_var::s_count,
      os_mutex_count()
    ));
  }

  log_info("Shutdown completed; log sequence number ", srv_shutdown_lsn);

  srv_was_started = false;

  InnoDB::modules_var_init();
  InnoDB::var_init();

  return DB_SUCCESS;
}
