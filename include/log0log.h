/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

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

#pragma once

// #include "api0api.h"
#include "log0types.h"
#include "log_buffer.h"
// #include "mach0data.h"
// #include "mtr0mtr.h"
#include "sync0sync.h"

struct Log {

  /**
   * Constructor
   *
   * @param[in] size The size of the log buffer in bytes. Must be a multiple of sizeof(Log_phy_block).
   */
  explicit Log(size_t size) noexcept;

  /**
   * Destructor */
  ~Log() noexcept;

  /** Opens the files that make up the log virtual file.
   *
   * @param[in] n_files The number of files to open.
   * @param[in] file_size The size of each file in bytes.
   *
   * @return True if the files are opened successfully, false otherwise.
   */
  [[nodiscard]] bool open(ulint n_files, off_t file_size) noexcept;

  /**
   * Closes the files that make up the log virtual file.
   *
   * @param[in] recovery Recovery flag.
   *
   * @return True if the files are closed successfully, false otherwise.
   */
  [[nodiscard]] lsn_t close(ib_recovery_t recovery) noexcept;

  /**
   * Acquire the log mutex.
   */
  void acquire() const noexcept {
    ut_ad(!mutex_own(&m_mutex));
    mutex_enter(&m_mutex);
  }

  /**
   * Releases the log mutex.
   */
  void release() const noexcept {
    ut_ad(mutex_own(&m_mutex));
    mutex_exit(&m_mutex);
  }

  /**
   * Reads a log segment to a buffer.
   *
   * @param[in, out] buffer The buffer where the log segment will be read into.
   * @param[in] start_lsn The start lsn of the log segment to read.
   * @param[in] end_lsn The end lsn of the log segment to read.
   * @param[out] scanned_lsn The lsn up to which the log segment has been read.
   *
   * @return True if the log segment is read successfully, false otherwise.
   */
  [[nodiscard]] bool read(Log_buffer &buffer, lsn_t start_lsn, lsn_t end_lsn, lsn_t *scanned_lsn) noexcept;

  /** Gets the current lsn.
  *
  * @return	current lsn
  */
  [[nodiscard]] lsn_t get_lsn() const noexcept { return m_lsn.load(); }

  /**
  * @brief Completes an i/o to a log file.
  */
  void io_complete(std::function<void()> callback) noexcept;

  /**
  * @brief Sets the global variable log_fsp_current_free_limit.
  * Also makes a checkpoint, so that we know that the limit has been written to a log checkpoint field on disk.
  */
  void do_checkpoint() noexcept;

  /**
  * @brief Appemd to the log buffer.
  * It is assumed that the caller holds the log mutex.
  *
  * @param[in] ptr Data to append
  * @param[in] len Length of the data to append.
  */
  void append(const byte *ptr, ulint len) noexcept;

  /**
  * @brief Checks that the log has been written to the log
  * file up to the last log entry written to the buffer. If there is
  * a flush running, it waits and checks if the flush flushed enough.
  * If not, starts a new flush.
  *
  * @param[in] lsn The log sequence number up to which the log should be
  *   written. Use IB_UINT64_T_MAX if not specified.
  * @param[in] async True if the operation should be asynchronous.
  * @param[in]  sync_to_disk True if we want the written log also to be forced to disk.
  */
  void write_up_to(lsn_t lsn, bool async, bool sync_to_disk) noexcept;

  /**
  * @brief Does a synchronous flush of the log buffer to disk.
  */
  void write() noexcept;

  /**
  * @brief Writes the log buffer to the log file and if 'flush' is set
  * it forces a flush of the log file as well. This is meant to be
  * called from background master thread only as it does not wait
  * for the write (+ possible flush) to finish.
  *
  * @param[in] flush True if the logs should be flushed to disk.
  */
  void background_write(bool sync_to_disk) noexcept;

  /**
  * @brief Advances the smallest lsn for which there are unflushed
  * dirty blocks in the buffer pool and also may make a new checkpoint.
  * Note: This function may only be called if the calling thread
  * owns no synchronization objects!
  *
  * @param[in] new_oldest Try to advance oldest_modified_lsn at least to this lsn.
  * @param[in] sync True if synchronous operation is desired.
  *
  * @return False if there was a flush batch of the same type running,
  * which means that we could not start this flush batch.
  */
  [[nodiscard]] bool preflush_pool_modified_pages(lsn_t new_oldest, bool sync) noexcept;

  /**
  * @brief Makes a checkpoint.
  * Note: This function does not flush dirty blocks from the buffer pool,
  * it only checks the lsn of the oldest modification in the pool and
  * writes information about the lsn in log files.
  *
  * @param[in] sync True if synchronous operation is desired.
  * @param[in] write_always The function normally checks if the new
  *   checkpoint would have a greater lsn than the previous one:
  *   if not, then no physical write is done. By setting this parameter
  *   true, a physical write will always be made to log files.
  *
  * @return True if success, false if a checkpoint write was already running.
  */
  [[nodiscard]] bool checkpoint(bool sync, bool write_always) noexcept;

  /**
  * @brief Makes a checkpoint at a given lsn or later.
  *
  * @param[in] lsn Make a checkpoint at this or a later lsn. Use IB_UINT64_T_MAX
  * to make a checkpoint at the latest lsn.
  * @param[in] write_always The function normally checks if the new checkpoint would
  * have a greater lsn than the previous one: if not, then no physical write is done.
  * By setting this parameter true, a physical write will always be made to log files.
  */
  void make_checkpoint_at(lsn_t lsn, bool write_always) noexcept;

  /**
  * Checks that there is enough free space in the log vfile to start a new query step.
  * Flushes the log buffer or makes a new checkpoint if necessary. NOTE: this
  * function may only be called if the calling thread doesn't own any latches.
  void check_margins() noexcept;

  /**
  * @brief Peeks the current lsn.
  *
  * @param lsn The output parameter where the current lsn will be stored if the function returns true.
  * @return True if success, false if could not get the log system mutex.
  */
  [[nodiscard]] bool peek_lsn(lsn_t *lsn) noexcept;

  /**
  * Shutdown log system but doesn't release all the memory.
  */
  void shutdown() noexcept;

  /**
  * Create the log instance.
  *
  * @param[in] log_buffer_size The size of the log buffer in bytes.
  *
  * @return The log instance.
  */
  [[nodiscard]] static Log *create(size_t log_buffer_size) noexcept;

  /**
  *  Free the log system data structures.
  *
  * @param[in,own] log Log instance to destroy.
  */
  static void destroy(Log *&log) noexcept;

  /**
   * Returns the oldest modified block LSN in the pool, or m_lsn if none exists.
   *
   * @return LSN of oldest modification
   */
  [[nodiscard]] lsn_t buf_pool_get_oldest_modification() noexcept;

  /**
    * Calculates the recommended highest values for lsn - last_checkpoint_lsn,
    * lsn - buf_get_oldest_modification(), and lsn - max_archive_lsn_age.
    *
    * @return error value false if the smallest vfile is too small to
    *  accommodate the number of OS threads in the database server */
  [[nodiscard]] bool calc_max_ages() noexcept;

  /**
   * Completes a checkpoint.
   */
  void sync_checkpoint_complete() noexcept;

  /**
   * Completes an asynchronous checkpoint info write i/o to a log file.
   */
  void async_checkpoint_complete() noexcept;

  /**
   * Writes the checkpoint info to a vfile header.
   *
   */
  void checkpoint() noexcept;

  /** Current log sequence number. */
  std::atomic<lsn_t> m_lsn{};

  /** Log buffer for reading and writing log records. */
  Log_buffer m_buffer;

  /** Mutex protecting the log */
  mutable mutex_t m_mutex{};

  /** Log virtual file */
  Log_vfile m_vfile;

  /** First LSN not yet written to the log file; for this
  to be advanced, it is enough that the write I/O has been completed. */
  lsn_t m_lwm{};

  /** End LSN for the current running write */
  lsn_t m_current_write_lsn{};

  /** End LSN for the current running write + flush operation */
  lsn_t m_current_flush_lsn{};

  /** How far we have written the log AND forced to disk */
  lsn_t m_forced_to_disk_lsn{};

  /** This event is in the reset state when a sync or a write is
  running; a thread should wait for this without owning the log
  mutex, but NOTE that to set or reset this event, the thread MUST
  own the log mutex! */
  Cond_var *m_no_flush_event{};

  /** This event is reset when the flush or write has not yet completed.
  When this is set; a thread should wait for this without owning
  the log mutex, but NOTE that to set or reset this event, the thread
  MUST own the log mutex! */
  Cond_var *m_one_flushed_event{};

  /** Total number of calls to append() */
  size_t m_log_write_requests{};

  /** Fields involved in checkpoints @{ */
  Log_checkpoint m_checkpoint{};

  /** Disable writes to the log file */
  bool m_disable_writes{false};
};

extern Log *log_sys;

using log_t = Log;

extern bool wal_io_system_init();

extern void wal_io_system_shutdown();
