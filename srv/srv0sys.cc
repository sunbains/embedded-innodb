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

/** @file srv/srv0sys.cc
System: creates, starts and shuts down the sub-systems of one InnoDB instance

Created 2/16/1996 Heikki Tuuri
*************************************************************************/

#include "srv0sys.h"

#include "btr0btr.h"
#include "buf0buf.h"
#include "buf0dblwr.h"
#include "buf0flu.h"
#include "dict0dict.h"
#include "dict0store.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "lock0lock.h"
#include "log0log.h"
#include "log0recv.h"
#include "log0wal.h"
#include "mtr0mtr.h"
#include "os0aio.h"
#include "os0file.h"
#include "os0sync.h"
#include "os0thread.h"
#include "page0page.h"
#include "pars0pars.h"
#include "row0ins.h"
#include "row0sel.h"
#include "row0undo.h"
#include "row0upd.h"
#include "row0vers.h"
#include "srv0srv.h"
#include "sync0sync.h"
#include "trx0purge.h"
#include "trx0sys.h"
#include "trx0undo.h"

#include <filesystem>

/** System tablespace initial size.  */
constexpr ulint SYSTEM_IBD_FILE_INITIAL_SIZE = 32 * 1024 * 1024;

/** Argument of an i/o handler thread. */
struct IoHandler {
  System *m_sys{};

  /** The Aio segment this thread serves. */
  ulint m_segment{};
};

/** I/O handler thread: serves one Aio segment until Aio is shut down.
@param[in] arg                  IoHandler. */
static void *io_handler_thread(void *arg) noexcept {
  auto handler = static_cast<IoHandler *>(arg);

  while (handler->m_sys->m_fil->aio_wait(handler->m_segment)) {}

  /* We count the number of threads in os_thread_exit(). A created
  thread should always use that to exit and not use return() to exit. */
  os_thread_exit();

  return nullptr;
}

DbErr Config::normalize() noexcept {
  if (m_log.m_n_files < 2) {
    log_err("There must be at least two log files");
    return DB_ERROR;
  }

  if (m_log.m_n_files * (m_log.m_file_size / UNIV_PAGE_SIZE) >= 262144) {
    log_err("Combined size of log files must be < 4 GB");
    return DB_ERROR;
  }

  if (1 + m_aio.m_n_read_threads + m_aio.m_n_write_threads > SRV_MAX_N_IO_THREADS) {
    log_err(std::format("At most {} i/o threads are supported", SRV_MAX_N_IO_THREADS));
    return DB_ERROR;
  }

  if (m_fil.m_data_home.empty()) {
    log_err("Data home directory is not set");
    return DB_ERROR;
  }

  if (m_fil.m_data_home.back() != SRV_PATH_SEPARATOR) {
    m_fil.m_data_home.push_back(SRV_PATH_SEPARATOR);
  }

  if (m_lock.m_table_size == 0) {
    m_lock.m_table_size = 5 * (m_buf_pool.m_size / UNIV_PAGE_SIZE);
  }

  /* The maximum number of threads which can wait for a semaphore inside
  InnoDB: the size of the sync wait array and of the thread table. */
  if (m_srv.m_max_n_threads == 0) {
    if (m_buf_pool.m_size >= 1000 * 1024 * 1024) {
      m_srv.m_max_n_threads = 8192;
    } else if (m_buf_pool.m_size >= 8 * 1024 * 1024) {
      m_srv.m_max_n_threads = 4096;
    } else {
      m_srv.m_max_n_threads = 128;
    }
  }

#ifdef UNIV_DEBUG
  /* We have observed deadlocks with a 5MB buffer pool but the actual lower
  limit could very well be a little higher. */
  if (m_buf_pool.m_size <= 5 * 1024 * 1024) {
    log_warn(std::format(
      "Small buffer pool size ({}M), the flst_validate() debug function "
      "can cause a deadlock if the buffer pool fills up.",
      m_buf_pool.m_size / 1024 / 1024
    ));
  }
#endif /* UNIV_DEBUG */

  return DB_SUCCESS;
}

System::System() noexcept = default;

System::~System() noexcept {
  destroy();
}

DbErr System::init(const Config &config) noexcept {
  ut_a(m_srv == nullptr);

  m_config = config;

  if (auto err = m_config.normalize(); err != DB_SUCCESS) {
    return err;
  }

  IF_DEBUG(log_warn("!!!!!!!! UNIV_DEBUG switched on !!!!!!!!!");)

  IF_SYNC_DEBUG(log_warn("!!!!!!!! UNIV_SYNC_DEBUG switched on !!!!!!!!!");)

  os_sync_init();
  sync_init(m_config.m_srv.m_max_n_threads);
  os_file_init();

  m_aio = Aio::create(m_config.m_aio);
  m_fil = std::make_unique<Fil>(*this);
  m_buf_pool = std::make_unique<BufPool>(*this);

  if (!m_buf_pool->open(m_config.m_buf_pool.m_size)) {
    log_err("Cannot allocate the memory for the buffer pool");
    destroy();
    return DB_OUT_OF_MEMORY;
  }

  m_log = std::make_unique<Log>(*this);


  m_recv = std::make_unique<RecvSys>(*this);
  m_fsp = std::make_unique<Fsp>(*this);
  m_trx_sys = std::make_unique<TrxSys>(*this);
  m_lock_sys = std::make_unique<LockSys>(*this);
  m_btree = std::make_unique<Btree>(*this);
  m_undo = std::make_unique<Undo>(*this);
  m_dict = std::make_unique<Dict>(*this);
  m_dblwr = std::make_unique<Dblwr>(*this);
  m_row_upd = std::make_unique<RowUpdate>(*this);
  m_row_ins = std::make_unique<RowInsert>(*this);
  m_row_sel = std::make_unique<RowSel>(*this);
  m_row_undo = std::make_unique<RowUndo>(*this);
  m_row_vers = std::make_unique<RowVers>(*this);
  m_srv = std::make_unique<Srv>(*this);

  return DB_SUCCESS;
}

/** Destroy a sub-system. Unlike unique_ptr::reset(), the pointer stays set
while the destructor runs, so the sub-system can still reach itself and its
parts through m_sys. */
template <typename T>
static void destroy_subsystem(std::unique_ptr<T> &ptr) noexcept {
  if (ptr != nullptr) {
    std::default_delete<T>{}(ptr.get());
    (void)ptr.release();
  }
}

void System::destroy() noexcept {
  if (m_srv == nullptr && m_aio == nullptr) {
    return;
  }

  /* Reverse creation order. The dictionary must go before the buffer pool
  is closed. */
  destroy_subsystem(m_srv);
  destroy_subsystem(m_row_vers);
  destroy_subsystem(m_row_undo);
  destroy_subsystem(m_row_sel);
  destroy_subsystem(m_row_ins);
  destroy_subsystem(m_row_upd);
  destroy_subsystem(m_dblwr);
  destroy_subsystem(m_dict);
  destroy_subsystem(m_undo);
  destroy_subsystem(m_btree);
  destroy_subsystem(m_lock_sys);
  destroy_subsystem(m_trx_sys);
  destroy_subsystem(m_fsp);
  destroy_subsystem(m_recv);

  if (m_buf_pool != nullptr) {
    m_buf_pool->close();
  }

  if (m_aio != nullptr) {
    m_aio->shutdown();
  }

  destroy_subsystem(m_fil);
  destroy_subsystem(m_log);
  destroy_subsystem(m_aio);
  destroy_subsystem(m_buf_pool);

  os_file_free();
  sync_close();
  os_sync_free();
  pars_close();
}

/**
 * @brief Creates a log file in the log group.
 *
 * @param[in] sys               The instance.
 * @param[in] filename          Log file name to create
 *
 * @return DB_SUCCESS or error code
 */
static DbErr create_log_file(System &sys, const std::string &filename) noexcept {
  ut_a(!filename.empty());

  const auto &config = sys.m_config.m_log;

  bool success{};
  auto fh = os_file_create(filename.c_str(), OS_FILE_CREATE, OS_FILE_NORMAL, OS_LOG_FILE, sys.m_config.m_fil.m_flush_method, &success);

  if (!success) {
    return os_file_get_last_error(false) != OS_FILE_ALREADY_EXISTS ? DB_ERROR : DB_TABLE_EXISTS;
  }

  log_warn(std::format("Log file {} did not exist: creating a new log file", filename));

  log_info(std::format("Setting log file {} size to {} MB", filename, config.m_file_size / (1024 * 1024)));

  log_info("Database physically writes the file full: wait...");

  success = os_file_set_size(filename.c_str(), fh, config.m_file_size);

  {
    auto success = os_file_close(fh);
    ut_a(success);
  }

  if (!success) {
    log_err(std::format("Can't create {}: probably out of disk space", filename));
    return DB_ERROR;
  }

  sys.m_fil->node_create(filename.c_str(), config.m_file_size / UNIV_PAGE_SIZE, SRV_LOG_SPACE_FIRST_ID, false);

  return DB_SUCCESS;
}

/**
 * @brief Opens a log file.
 *
 * @param[in] sys               The instance.
 * @param[in] filename          Log file name to open
 *
 * @return DB_SUCCESS or error code
 */
static DbErr open_log_file(System &sys, const std::string &filename) noexcept {
  ut_a(!filename.empty());

  const auto &config = sys.m_config.m_log;

  bool success{};
  auto fh = os_file_create(filename.c_str(), OS_FILE_OPEN, OS_FILE_AIO, OS_LOG_FILE, sys.m_config.m_fil.m_flush_method, &success);

  if (!success) {
    log_err("Cannot open log file ", filename);

    return DB_ERROR;
  }

  off_t size{};
  success = os_file_get_size(fh, &size);
  ut_a(success);

  if (size != off_t(config.m_file_size)) {

    log_err(std::format("Log file {} is of different size {} bytes than the configured {} bytes!", filename, size, config.m_file_size));

    return DB_ERROR;
  }

  success = os_file_close(fh);
  ut_a(success);

  sys.m_fil->node_create(filename.c_str(), config.m_file_size / UNIV_PAGE_SIZE, SRV_LOG_SPACE_FIRST_ID, false);

  return DB_SUCCESS;
}

/**
 * @brief Creates the system tablespace.
 *
 * @param[in] sys               The instance.
 * @param[in] path              Path to the system tablespace
 *
 * @return DB_SUCCESS or error code
 */
static DbErr create_system_tablespace(System &sys, const std::string &path) noexcept {
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
        home_dir.generic_string()
      ));
      return DB_ERROR;
    }
  }

  if (fs::exists(filename) && fs::status(filename).type() == fs::file_type::regular) {
    log_warn(std::format("System tablespace '{}' already exists", filename.generic_string()));
    return DB_TABLE_EXISTS;
  }

  log_info(std::format("Creating the system tablespace: 'system.idb' of size {} MB ", SYSTEM_IBD_FILE_INITIAL_SIZE / (1024 * 1024)));

  bool success{};
  auto fh = os_file_create(filename.c_str(), OS_FILE_CREATE, OS_FILE_NORMAL, OS_DATA_FILE, sys.m_config.m_fil.m_flush_method, &success);

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
 * @param[in] sys               The instance.
 * @param[in] path              Path to the system tablespace
 * @param[out] flushed_lsn      Max of flushed lsn values in data files
 *
 * @return DB_SUCCESS or error code
 */
static DbErr open_system_tablespace(System &sys, const std::string &path, lsn_t &flushed_lsn) noexcept {
  namespace fs = std::filesystem;

  ut_a(!path.empty());

  fs::path filename(path);

  /* Must exist if we've been asked to open the file. */
  ut_a(fs::status(filename).type() == fs::file_type::regular);

  bool success{};
  auto fh = os_file_create(filename.c_str(), OS_FILE_OPEN, OS_FILE_NORMAL, OS_DATA_FILE, sys.m_config.m_fil.m_flush_method, &success);

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

  sys.m_fil->read_flushed_lsn(fh, flushed_lsn);

  {
    auto success = os_file_close(fh);
    ut_a(success);
  }

  {
    auto success = sys.m_fil->space_create(filename.c_str(), SYS_TABLESPACE, 0, FIL_TABLESPACE);
    ut_a(success);
    ut_a(sys.m_fil->validate());
  }

  sys.m_fil->node_create(filename.c_str(), size, 0, false);

  return DB_SUCCESS;
}

/**
 * All threads end up waiting for certain events. Put those events to the
 * signaled state. Then the threads will exit themselves.
 *
 * @param[in] sys               The instance.
 *
 * @return	true if all threads exited.
 */
static bool threads_shutdown(System &sys) noexcept {
  sys.m_srv->m_shutdown_state = SRV_SHUTDOWN_EXIT_THREADS;

  for (ulint i{}; i < 1000; ++i) {
    /* Let the lock timeout thread exit */
    os_event_set(sys.m_srv->m_lock_timeout_thread_event);

    /* The error monitor thread exits automatically. We wake the master
    thread so that it exits */
    sys.m_srv->wake_master_thread();

    sys.m_aio->shutdown();

    if (os_thread_count.load(std::memory_order_relaxed) == 0) {
      /* All the threads have exited or are just exiting; NOTE that the
      threads may not have completed their exit yet. */
      return true;
    }
  }

  log_warn(std::format("{} threads created by InnoDB had not exited at shutdown!", os_thread_count.load(std::memory_order_relaxed)));

  return false;
}

/**
 * @brief Empties the logs and marks files at shutdown.
 *
 * @param[in] sys               The instance.
 * @param[in] shutdown          How much work to do before stopping.
 */
static void prepare_for_shutdown(System &sys, ib_shutdown_t shutdown) noexcept {
  auto &srv = *sys.m_srv;
  auto &log = *sys.m_log;
  auto &trx_sys = *sys.m_trx_sys;
  auto &buf_pool = *sys.m_buf_pool;

  if (sys.m_config.m_srv.m_verbose_log) {
    log_info("Starting shutdown...");
  }

  /* Wait until the master thread and all other operations are idle: our
  algorithm only works if the server is idle at shutdown */

  srv.m_shutdown_state = SRV_SHUTDOWN_CLEANUP;

  lsn_t lsn;

  for (;;) {
    os_thread_sleep(100000);

    mutex_enter(&trx_sys.m_mutex);

    /* We need the monitor threads to stop before we proceed with a normal
    shutdown. In case of very fast shutdown, however, we can proceed without
    waiting for monitor threads. */

    if (shutdown != IB_SHUTDOWN_NO_BUFPOOL_FLUSH && (srv.m_error_monitor_active || srv.m_lock_timeout_active || srv.m_monitor_active)) {

      mutex_exit(&trx_sys.m_mutex);
      continue;
    }

    /* Check that there are no longer transactions. We need this wait even
    for the 'very fast' shutdown, because the InnoDB layer may have
    committed or prepared transactions and we don't want to lose them. */

    if (trx_sys.m_n_user_trx > 0 || !trx_sys.m_trx_list.empty()) {

      mutex_exit(&trx_sys.m_mutex);

      continue;
    }

    if (shutdown == IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
      /* In this fastest shutdown we do not flush the buffer pool: it is
      essentially a 'crash' of the InnoDB server. Make sure that the log is
      all flushed to disk, so that we can recover all committed transactions
      in a crash recovery. We must not write the lsn stamps to the data files,
      since at a startup InnoDB deduces from the stamps if the previous
      shutdown was clean. */

      log.buffer_flush_to_disk();

      mutex_exit(&trx_sys.m_mutex);

      return; /* We SKIP ALL THE REST !! */
    }

    /* Check that the master thread is suspended */
    if (srv.m_n_threads_active[SRV_MASTER] != 0) {

      mutex_exit(&trx_sys.m_mutex);

      continue;
    }

    mutex_exit(&trx_sys.m_mutex);

    log.acquire();

    if (log.m_checkpoint.m_n_pending_writes || log.m_n_pending_writes) {

      log.release();

      continue;
    }

    log.release();

    if (buf_pool.is_io_pending()) {

      continue;
    }

    log.make_checkpoint_at(IB_UINT64_T_MAX, true);

    log.acquire();

    lsn = log.m_lsn;

    if (lsn != log.m_checkpoint.m_last_lsn) {

      log.release();

      continue;
    }

    log.release();

    mutex_enter(&trx_sys.m_mutex);

    /* Check that the master thread has stayed suspended */
    if (srv.m_n_threads_active[SRV_MASTER] != 0) {
      log_warn("The master thread woke up during shutdown");

      mutex_exit(&trx_sys.m_mutex);

      continue;
    }

    mutex_exit(&trx_sys.m_mutex);

    sys.m_fil->flush_file_spaces(FIL_TABLESPACE);
    sys.m_fil->flush_file_spaces(FIL_LOG);

    /* The call Fil::write_flushed_lsn_to_data_files() will pass the buffer
    pool: therefore it is essential that the buffer pool has been completely
    flushed to disk! (We do not call it if the 'very fast' shutdown is
    enabled.) */

    if (buf_pool.all_freed()) {
      break;
    }
  }

  srv.m_shutdown_state = SRV_SHUTDOWN_LAST_PHASE;

  /* Make some checks that the server really is quiet */
  ut_a(lsn == log.m_lsn);
  ut_a(buf_pool.all_freed());
  ut_a(srv.m_n_threads_active[SRV_MASTER] == 0);

  if (lsn < srv.m_start_lsn) {
    log_err(std::format("Log sequence number at shutdown {} is lower than at startup {}", lsn, srv.m_start_lsn));
  }

  srv.m_shutdown_lsn = lsn;

  sys.m_fil->write_flushed_lsn_to_data_files(lsn);

  sys.m_fil->flush_file_spaces(FIL_TABLESPACE);

  sys.m_fil->close_all_files();

  /* Make some checks that the server really is quiet */
  ut_a(srv.m_n_threads_active[SRV_MASTER] == 0);
  ut_a(buf_pool.all_freed());
  ut_a(lsn == log.m_lsn);
}

void System::start_wal() noexcept {
  LogWalBackend::Config config;

  config.m_log_dir = m_config.m_log.m_group_home_dir;
  config.m_log_file = "redo.wal";
  config.m_pool_size = 4;
  config.m_max_file_size = off_t(m_config.m_log.m_n_files * m_config.m_log.m_file_size);
  config.m_n_blocks = std::max<size_t>(16, m_config.m_log.m_buffer_size / config.m_block_size);

  m_log->init_wal_backend(config);
}

void System::startup_abort(DbErr err) noexcept {
  m_srv->m_fast_shutdown = IB_SHUTDOWN_NORMAL;

  /* For fatal errors we want to avoid writing to the data files. */
  if (err != DB_FATAL) {
    prepare_for_shutdown(*this, IB_SHUTDOWN_NORMAL);
    m_fil->close_all_files();
  }

  threads_shutdown(*this);

  m_log->shutdown();

  destroy();
}

DbErr System::startup() noexcept {
  ut_a(m_srv != nullptr);
  ut_a(!m_srv->m_was_started);

  auto &srv = *m_srv;

  /* System tables are created in tablespace 0. Thus, we must temporarily
  suspend file per table. This is ok, because the server will not accept
  connections until this function has returned. */
  const auto file_per_table = m_fil->m_file_per_table;

  m_fil->m_file_per_table = false;

  // FIXME: Make this a configurable option
  ut_d(m_log->m_disable_writes = false);

  srv.m_is_being_started = true;

  srv.m_startup_is_before_trx_rollback_phase = true;

  /* Create the i/o handler threads: one for the log, the others for the
  read and write segments. */
  const auto n_file_io_threads = 1 + m_config.m_aio.m_n_read_threads + m_config.m_aio.m_n_write_threads;

  m_io_handlers.resize(n_file_io_threads);

  for (ulint i{}; i < n_file_io_threads; ++i) {
    os_thread_id_t id;

    m_io_handlers[i] = IoHandler{this, i};

    os_thread_create(io_handler_thread, &m_io_handlers[i], &id);
  }

  bool create_new_db{};
  lsn_t max_flushed_lsn{};
  DbErr err{DB_SUCCESS};

  {
    namespace fs = std::filesystem;

    fs::path home(m_config.m_fil.m_data_home);

    if (!fs::exists(home) || fs::status(home).type() != fs::file_type::directory) {
      log_err("Data home directory does not exist or is not a directory");
      return DB_ERROR;
    }

    auto filename{home / "system.ibd"};

    if (!fs::exists(filename)) {
      err = create_system_tablespace(*this, filename);

      if (err == DB_SUCCESS) {
        create_new_db = true;
      } else if (err != DB_TABLE_EXISTS) {
        startup_abort(err);
        return DB_ERROR;
      }
    }

    err = open_system_tablespace(*this, filename, max_flushed_lsn);

    if (err != DB_SUCCESS) {
      startup_abort(err);
      return DB_ERROR;
    }
  }

  bool log_opened{};
  bool log_created{};

  /* Note: Currently we support a single log group (0). */
  const auto &log_dir = m_config.m_log.m_group_home_dir;

  {
    auto success = m_fil->space_create(log_dir.c_str(), SRV_LOG_SPACE_FIRST_ID, 0, FIL_LOG);
    ut_a(success);
    ut_a(m_fil->validate());
  }

  for (ulint i{}; i < m_config.m_log.m_n_files; ++i) {
    namespace fs = std::filesystem;

    fs::path filename(fs::path(log_dir) / std::format("ib_logfile{}", i));

    if (fs::exists(filename) && fs::status(filename).type() == fs::file_type::regular) {
      err = open_log_file(*this, filename.generic_string());
      log_opened = true;
    } else {
      err = create_log_file(*this, filename.generic_string());
      log_created = true;
    }

    if (err != DB_SUCCESS) {
      startup_abort(err);
      return err;
    }

    if (i == 0) {
      m_log->group_init(i, m_config.m_log.m_n_files, m_config.m_log.m_file_size, SRV_LOG_SPACE_FIRST_ID);
    }

    if ((log_opened && create_new_db) || (log_opened && log_created)) {
      log_err(
        "All log files must be created at the same time. All log files must be"
        " created also in database creation. If you want bigger or smaller"
        " log files, shut down the database and make sure there were no errors"
        " in shutdown. Then delete the existing log files. Reconfigure InnoDB"
        " and start the database again."
      );

      startup_abort(DB_ERROR);
      return DB_ERROR;
    }
  }

  /* Open all log files and the system tablespace: we keep them open
  until database shutdown */

  m_fil->open_log_and_system_tablespace();

  if (log_created && !create_new_db) {
    if (max_flushed_lsn < lsn_t(1000)) {
      log_err(
        "Cannot initialize created log files because data files are corrupt,"
        " or new data files were created when the database was started previous"
        " time but the database was not shut down normally after that."
      );

      startup_abort(DB_ERROR);
      return DB_ERROR;
    }

    m_log->acquire();

    recv_reset_logs(max_flushed_lsn, true);

    m_log->release();
  }

  const auto force_recovery = m_config.m_recv.m_force_recovery;

  if (create_new_db) {
    start_wal();

    srv.m_start_lsn = m_log->get_lsn();

    Mtr mtr;

    mtr.start();

    m_fsp->header_init(SYS_TABLESPACE, SYSTEM_IBD_FILE_INITIAL_SIZE / UNIV_PAGE_SIZE, &mtr);

    mtr.commit(*this);

    err = m_trx_sys->create_system_tablespace();

    if (err == DB_SUCCESS) {
      err = m_trx_sys->start(force_recovery);
    }

    if (err == DB_SUCCESS) {
      err = m_dict->create_instance();
    }

    if (err == DB_SUCCESS) {
      err = m_dict->open(false);
    }

    if (err != DB_SUCCESS) {
      startup_abort(err);
      return DB_ERROR;
    }

    srv.m_startup_is_before_trx_rollback_phase = false;

    /* Create the doublewrite buffer for the system tablespace */
    err = m_dblwr->initialize();

    if (err != DB_SUCCESS) {
      startup_abort(err);
      return DB_ERROR;
    }

    /* Flush the modified pages to disk and make a checkpoint */
    m_log->make_checkpoint_at(IB_UINT64_T_MAX, true);

  } else {

    /* Open and restore the doublewrite buffer. */
    std::pair<page_no_t, page_no_t> offsets{};

    if (!m_dblwr->check_if_exists(offsets)) {
      err = m_dblwr->initialize();

      if (err != DB_SUCCESS) {
        startup_abort(err);
        return DB_ERROR;
      }
    } else {
      m_dblwr->m_block1 = offsets.first;
      m_dblwr->m_block2 = offsets.second;
    }

    log_warn("Reading tablespace information from the .ibd files...");

    /* Recursively scan to a depth of 2. InnoDB needs to do this because the DD
    can't be accessed until recovery is done. So we have this simplistic scheme. */
    m_fil->load_single_table_tablespaces(m_config.m_fil.m_data_home, force_recovery, 2);

    /* Restore the pages in data files from the doublewrite buffer if possible */
    if (force_recovery < IB_RECOVERY_NO_LOG_REDO) {
      log_warn("Restoring possible half-written data pages from the doublewrite buffer...");
      m_dblwr->recover_pages();
    }

    /* We always try to do a recovery, even if the database had been shut
    down normally: this is the normal startup path */

    err = recv_recovery_from_checkpoint_start(*this, max_flushed_lsn);

    if (err != DB_SUCCESS) {
      startup_abort(err);
      return DB_ERROR;
    }

    start_wal();

    err = m_trx_sys->start(force_recovery);

    {
      auto free_limit = m_fsp->init_system_space_free_limit();
      log_info(std::format("Free limit of system.ibd set to {} MB", free_limit));
    }

    srv.m_startup_is_before_trx_rollback_phase = false;

    /* recovery_finish() needs the trx lists which are initialized by
    TrxSys::start(). */

    recv_recovery_from_checkpoint_finish(*this);

    if (force_recovery <= IB_RECOVERY_NO_TRX_UNDO) {
      /* In a crash recovery, we check that the info in data dictionary is
      consistent with what we already know about space id's from the call of
      Fil::load_single_table_tablespaces().

      In a normal startup, we create the space objects for every table in the
      InnoDB data dictionary that has an .ibd file.

      We also determine the maximum tablespace id used. */

      if (auto err = m_dict->open(m_recv->m_needed_recovery); err != DB_SUCCESS) {
        startup_abort(err);
        return DB_ERROR;
      }
    }

    recv_recovery_rollback_active(*this);
  }

  log_info("Max allowed record size ", page_get_free_space_of_empty() / 2);

  srv.m_is_being_started = false;

  ut_a(err == DB_SUCCESS);

  err = m_dict->m_store.create_or_check_foreign_constraint_tables();

  if (err != DB_SUCCESS) {
    startup_abort(err);
    return DB_ERROR;
  }

  srv.start_threads();

  {
    const auto size = m_fsp->get_system_space_size();
    log_info(std::format("system.ibd file size in the header is {} pages", size));
  }

  log_info(std::format("InnoDB {} started; log sequence number {}", VERSION, srv.m_start_lsn));

  if (force_recovery != IB_RECOVERY_DEFAULT) {
    log_warn(std::format("!!! force_recovery is set to {} !!!", (int)force_recovery));
  }

  m_fil->m_file_per_table = file_per_table;

  srv.m_was_started = true;

  return DB_SUCCESS;
}

DbErr System::shutdown(ib_shutdown_t shutdown) noexcept {
  ut_a(m_srv != nullptr && m_srv->m_was_started);

  /* Tell the master thread how much work to do. */
  m_srv->m_fast_shutdown = shutdown;

  /* 1. Flush the buffer pool to disk, write the current lsn to the
  tablespace header(s), and copy all log data to archive. The step 1 is the
  real InnoDB shutdown. The remaining steps just free data structures after
  the shutdown. */

  if (shutdown == IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
    log_warn(
      "User has requested a very fast shutdown without flushing"
      " the InnoDB buffer pool to data files. At the next startup"
      " InnoDB will do a crash recovery!"
    );
  }

  prepare_for_shutdown(*this, shutdown);

  /* In a 'very fast' shutdown, we do not need to wait for these threads to
  die; all which counts is that we flushed the log; a 'very fast' shutdown is
  essentially a crash. */

  if (shutdown == IB_SHUTDOWN_NO_BUFPOOL_FLUSH) {
    return DB_SUCCESS;
  }

  threads_shutdown(*this);

  m_log->shutdown();

  destroy();

  return DB_SUCCESS;
}
