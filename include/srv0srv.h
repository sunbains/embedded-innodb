/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2008, 2009, Google Inc.
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

/** @file include/srv0srv.h
The server main program

Created 10/10/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"
#include "srv0sys.h"

#include "innodb0types.h"
#include "os0sync.h"
#include "que0types.h"
#include "sync0sync.h"
#include "trx0types.h"

#include <array>
#include <vector>

struct Aio;



/** Shutdown state */
enum SrvShutdownState {
  /** Database running normally */
  SRV_SHUTDOWN_NONE = 0,

  /** Cleaning up in logs_empty_and_mark_files_at_shutdown() */
  SRV_SHUTDOWN_CLEANUP,

  /** Last phase after ensuring that the buffer pool can be freed:
  flush all file spaces and close all files */
  SRV_SHUTDOWN_LAST_PHASE,

  /** Exit all threads */
  SRV_SHUTDOWN_EXIT_THREADS
};

/** Types of threads existing in the system. */
enum SrvThreadType {
  SRV_NONE = 0,

  /** Threads serving communication and queries */
  SRV_COM = 1,

  /** Thread serving console */
  SRV_CONSOLE,

  /** Threads serving parallelized queries and queries released from lock wait
   */
  SRV_WORKER,

#if 0
  /* Utility threads */

  /** Thread flushing dirty buffer blocks */
  SRV_BUFFER,

  /** Threads finishing a recovery */
  SRV_RECOVERY,

#endif

  /** The master thread, (whose type number must be biggest) */
  SRV_MASTER
};

constexpr ulint SRV_MAX_N_IO_THREADS = 32;

constexpr ulint SRV_LOG_SPACE_FIRST_ID = 0xFFFFFFF0UL;

/** In this structure we store status variables to be passed to the client. */
struct ExportVars {
  /** Pending reads */
  ulint innodb_data_pending_reads;

  /** Pending writes */
  ulint innodb_data_pending_writes;

  /** Pending fsyncs */
  ulint innodb_data_pending_fsyncs;

  /** Number of fsyncs so far */
  ulint innodb_data_fsyncs;

  /** Data bytes read */
  ulint innodb_data_read;

  /** I/O write requests */
  ulint innodb_data_writes;

  /** Data bytes written */
  ulint innodb_data_written;

  /** I/O read requests */
  ulint innodb_data_reads;

  /** Buffer pool size */
  ulint innodb_buffer_pool_pages_total;

  /** Data pages */
  ulint innodb_buffer_pool_pages_data;

  /** Dirty data pages */
  ulint innodb_buffer_pool_pages_dirty;

  /** Miscellanous pages */
  ulint innodb_buffer_pool_pages_misc;

  /** Free pages */
  ulint innodb_buffer_pool_pages_free;

#ifdef UNIV_DEBUG
  /** Latched pages */
  ulint innodb_buffer_pool_pages_latched;
#endif /* UNIV_DEBUG */

  /** BufPool::stat.n_page_gets */
  ulint innodb_buffer_pool_read_requests;

  /** srv_buf_pool_reads */
  ulint innodb_buffer_pool_reads;

  /** srv_buf_pool_wait_free */
  ulint innodb_buffer_pool_wait_free;

  /** srv_buf_pool_flushed */
  ulint innodb_buffer_pool_pages_flushed;

  /** srv_buf_pool_write_requests */
  ulint innodb_buffer_pool_write_requests;

  /** srv_read_ahead */
  ulint innodb_buffer_pool_read_ahead;

  /** srv_read_ahead evicted*/
  ulint innodb_buffer_pool_read_ahead_evicted;

  /** srv_dblwr_pages_written */
  ulint innodb_dblwr_pages_written;

  /** srv_dblwr_writes */
  ulint innodb_dblwr_writes;

  /** Always true for now */
  bool innodb_have_atomic_builtins;

  /** srv_log_waits */
  ulint innodb_log_waits;

  /** srv_log_write_requests */
  ulint innodb_log_write_requests;

  /** srv_log_writes */
  ulint innodb_log_writes;

  /** srv_os_log_written */
  ulint innodb_os_log_written;

  /** Fil::n_log_flushes */
  ulint innodb_os_log_fsyncs;

  /** srv_os_log_pending_writes */
  ulint innodb_os_log_pending_writes;

  /** Fil::n_pending_log_flushes */
  ulint innodb_os_log_pending_fsyncs;

  /** UNIV_PAGE_SIZE */
  ulint innodb_page_size;

  /** BufPool::stat.n_pages_created */
  ulint innodb_pages_created;

  /** BufPool::stat.n_pages_read */
  ulint innodb_pages_read;

  /** BufPool::stat.n_pages_written */
  ulint innodb_pages_written;

  /** srv_n_lock_wait_count */
  ulint innodb_row_lock_waits;

  /** srv_n_lock_wait_current_count */
  ulint innodb_row_lock_current_waits;

  /** srv_n_lock_wait_time / 1000 */
  int64_t innodb_row_lock_time;

  /** srv_n_lock_wait_time / 1000 / srv_n_lock_wait_count */
  ulint innodb_row_lock_time_avg;

  /** srv_n_lock_max_wait_time / 1000 */
  ulint innodb_row_lock_time_max;

  /** srv_n_rows_read */
  ulint innodb_rows_read;

  /** srv_n_rows_inserted */
  ulint innodb_rows_inserted;

  /** srv_n_rows_updated */
  ulint innodb_rows_updated;

  /** srv_n_rows_deleted */
  ulint innodb_rows_deleted;
};

struct Fil;
struct Fsp;
struct Undo;
struct Btree;
struct Dict;
struct Dblwr;
struct TrxSys;
struct LockSys;
struct RowSel;
struct RowVers;
struct RowUndo;
struct RowUpdate;
struct RowInsert;

/** Thread slot in the thread table */
struct SrvSlot {
  /** thread id */
  os_thread_id_t m_id{};

  /** thread handle */
  os_thread_t m_handle{};

  /** thread type: user, utility etc. */
  SrvThreadType m_type{SRV_NONE};

  /** true if this slot is in use */
  bool m_in_use{};

  /** true if the thread is waiting for the event of this slot */
  bool m_suspended{};

  /** time when the thread was suspended */
  ib_time_t m_suspend_time{};

  /** event used in suspending the thread when it has nothing to do */
  CondVar *m_event{};

  /** suspended query thread (only used for client threads) */
  QueThr *m_thr{};
};

/** Counters, exported as status variables. */
struct SrvStats {
  /** Amount of data read in total (in bytes) */
  ulint m_data_read{};

  /** Amount of data written in total (in bytes) */
  ulint m_data_written{};

  /** Number of log write requests done */
  ulint m_log_write_requests{};

  /** Number of physical writes to the log performed */
  ulint m_log_writes{};

  /** Amount of data written to the log files in bytes */
  ulint m_os_log_written{};

  /** Number of writes being done to the log files */
  ulint m_os_log_pending_writes{};

  /** Times we didn't have enough space in the log buffer and had to flush it */
  ulint m_log_waits{};

  /** Times the doublewrite buffer was flushed */
  ulint m_dblwr_writes{};

  /** Number of pages that have been flushed to the doublewrite buffer */
  ulint m_dblwr_pages_written{};

  /** Times we had to wait for a free page in the buffer pool */
  ulint m_buf_pool_wait_free{};

  /** Number of pages written from the buffer pool to disk */
  ulint m_buf_pool_flushed{};

  /** Number of buffer pool reads that led to the reading of a disk page */
  ulint m_buf_pool_reads{};

  ulint m_n_rows_inserted{};
  ulint m_n_rows_updated{};
  ulint m_n_rows_deleted{};
  ulint m_n_rows_read{};

  ulint m_n_lock_wait_count{};
  ulint m_n_lock_wait_current_count{};
  int64_t m_n_lock_wait_time{};
  ulint m_n_lock_max_wait_time{};
};

/** The server: background threads (master, lock timeout, monitors), the
thread table used to suspend user threads waiting for locks, the startup and
shutdown state, and the status counters. */
struct Srv {
  /** The instance this sub-system belongs to. */
  System &m_sys;

  explicit Srv(System &sys) noexcept;

  ~Srv() noexcept;

  Srv(const Srv &) = delete;
  Srv &operator=(const Srv &) = delete;

  /** Start the lock timeout, error monitor, monitor and master threads. */
  void start_threads() noexcept;

  /**
   * Releases threads of the type given from suspension in the thread table.
   * NOTE! The trx_sys mutex has to be reserved by the caller!
   *
   * @param[in] type	thread type
   * @param[in] n	number of threads to release
   *
   * @return number of threads released: this may be less than n if not
   *  enough threads were suspended at the moment
   */
  ulint release_threads(SrvThreadType type, ulint n) noexcept;

  /**
   * Tells the server that there has been activity in the database and wakes
   * up the master thread if it is suspended (not sleeping). There is a small
   * chance that the master thread stays suspended (we do not protect our
   * operation with the trx_sys mutex, for performace reasons).
   */
  void active_wake_master_thread() noexcept;

  /** Wakes up the master thread if it is suspended or being suspended. */
  void wake_master_thread() noexcept;

  /**
   * Puts a user OS thread to wait for a lock to be released. If an error
   * occurs during the wait trx->error_state associated with thr is
   * != DB_SUCCESS when we return. DB_LOCK_WAIT_TIMEOUT and DB_DEADLOCK
   * are possible errors. DB_DEADLOCK is returned if selective deadlock
   * resolution chose this transaction as a victim.
   *
   * @param[in] thr	query thread associated with the user OS thread
   */
  void suspend_user_thread(QueThr *thr) noexcept;

  /**
   * Releases a user OS thread waiting for a lock to be released, if the
   * thread is already suspended. NOTE! The trx_sys mutex has to be reserved
   * by the caller!
   *
   * @param[in] thr	query thread associated with the user OS thread
   */
  void release_user_thread_if_suspended(QueThr *thr) noexcept;

  /**
   * Enqueues a task to server task queue and releases a worker thread, if
   * there is a suspended one.
   *
   * @param[in] thr	query thread
   */
  void que_task_enqueue_low(QueThr *thr) noexcept;

  /**
   * Outputs to a file the output of the InnoDB Monitor.
   *
   * @param[in] out             output stream
   * @param[in] nowait          whether to wait for the trx_sys mutex
   * @param[out] trx_start      file position of the start of the list of
   *                            active transactions
   * @param[out] trx_end        file position of the end of the list of
   *                            active transactions
   *
   * @return false if not all information printed due to failure to
   *  obtain necessary mutex
   */
  [[nodiscard]] bool printf_innodb_monitor(FILE *out, bool nowait, ulint *trx_start, ulint *trx_end) noexcept;

  /** Function to pass InnoDB status variables to the client */
  void export_innodb_status() noexcept;

  /** Print the InnoDB monitor output once, at the next wake up of the
  monitor thread. */
  void request_monitor_print() noexcept;

  /** @return p percent of the configured i/o capacity, in pages. */
  [[nodiscard]] ulint pct_io(ulint p) const noexcept {
    return ulint(m_sys.m_config.m_buf_pool.m_io_capacity * (double(p) / 100.0));
  }

  /** At a shutdown this value climbs from SRV_SHUTDOWN_NONE to
  SRV_SHUTDOWN_CLEANUP and then to SRV_SHUTDOWN_LAST_PHASE, and so on */
  SrvShutdownState m_shutdown_state{SRV_SHUTDOWN_NONE};

  /** How much work System::shutdown() asked to do before stopping. */
  ib_shutdown_t m_fast_shutdown{IB_SHUTDOWN_NORMAL};

  /** true if the server is being started */
  bool m_is_being_started{};

  /** true if the server was successfully started */
  bool m_was_started{};

  /** true if the server is being started, before rolling back any
  incomplete transactions */
  bool m_startup_is_before_trx_rollback_phase{};

  /** Log sequence number immediately after startup */
  lsn_t m_start_lsn{};

  /** Log sequence number at shutdown */
  lsn_t m_shutdown_lsn{};

  /** Incremented whenever there is some user activity in the server */
  ulint m_activity_count{};

  /** Seconds a thread may wait for a latch before the server is
  considered hung; temporarily raised during long dictionary loads. */
  ulint m_fatal_semaphore_wait_threshold{};

  /** How much data manipulation language (DML) statements need to be
  delayed, in microseconds, in order to reduce the lagging of the purge
  thread. */
  ulint m_dml_needed_delay{};

  bool m_lock_timeout_active{};
  bool m_monitor_active{};
  bool m_error_monitor_active{};

  bool m_print_innodb_monitor{};
  bool m_print_innodb_tablespace_monitor{};
  bool m_print_innodb_table_monitor{};

  /** What the master thread is doing, for the monitor output. */
  const char *m_main_thread_op_info{""};

  /** When this event is set the lock timeout and monitor threads run. */
  CondVar *m_lock_timeout_thread_event{};

  /** Number of active threads by type, protected by the trx_sys mutex. */
  std::array<ulint, SRV_MASTER + 1> m_n_threads_active{};

  /** Threads used by parallel reads, across all ParallelReader instances. */
  std::atomic_size_t m_n_parallel_read_threads{};

  /** Counters, exported as status variables. */
  SrvStats m_stats{};

  /** Status variables to be passed to the client */
  ExportVars m_export_vars{};

 private:
  /** Thread entry point: runs F on the Srv instance passed as the argument. */
  template <void (Srv::*F)() noexcept>
  static void *thread_main(void *arg) noexcept {
    (static_cast<Srv *>(arg)->*F)();
    return nullptr;
  }

  void lock_timeout_thread() noexcept;
  void error_monitor_thread() noexcept;
  void monitor_thread() noexcept;
  void master_thread() noexcept;

  /** Prints counters for work done by the master thread. */
  void print_master_thread_info() noexcept;

  /** Refreshes the values used to calculate per-second averages. */
  void refresh_innodb_monitor_stats() noexcept;

  /** Flush the log buffer once a second in the background, so that not more
  than one second of transactions is lost in a crash when
  flush_at_trx_commit != 1. */
  void sync_log_buffer_in_background() noexcept;

  [[nodiscard]] SrvSlot *table_get_nth_slot(ulint index) noexcept;

  /** Reserves a slot in the thread table for the current thread.
  NOTE! The trx_sys mutex has to be reserved by the caller! */
  [[nodiscard]] ulint table_reserve_slot(SrvThreadType type) noexcept;

  /** Reserves a slot in the thread table for the current user OS thread.
  NOTE! The trx_sys mutex has to be reserved by the caller! */
  [[nodiscard]] SrvSlot *table_reserve_slot_for_user_thread() noexcept;

  /** Suspends the calling thread to wait for the event in its thread slot.
  NOTE! The trx_sys mutex has to be reserved by the caller! */
  [[nodiscard]] CondVar *suspend_thread(SrvSlot *slot) noexcept;

  /** Server thread table */
  std::vector<SrvSlot> m_threads;

  /** Table for client threads where they will be suspended to wait for locks */
  std::vector<SrvSlot> m_client_table;

  /** Number of threads by type, protected by the trx_sys mutex. */
  std::array<ulint, SRV_MASTER + 1> m_n_threads{};

  /** Task queue */
  UT_LIST_BASE_NODE_T(QueThr, queue) m_tasks{};

  Mutex m_innodb_monitor_mutex{};
  Mutex m_monitor_file_mutex{};

  time_t m_last_monitor_time{};

  /** Time of the last log flush by the master thread. */
  time_t m_last_log_flush_time{};

  ulint m_main_thread_process_no{};
  ulint m_main_thread_id{};

  /* The following count work done by the master thread. */
  ulint m_main_1_second_loops{};
  ulint m_main_sleeps{};
  ulint m_main_10_second_loops{};
  ulint m_main_background_loops{};
  ulint m_main_flush_loops{};
  ulint m_log_writes_and_flush{};

  /* Values at the last monitor output, for the per second averages. */
  ulint m_n_rows_inserted_old{};
  ulint m_n_rows_updated_old{};
  ulint m_n_rows_deleted_old{};
  ulint m_n_rows_read_old{};
};
