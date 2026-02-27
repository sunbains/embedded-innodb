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

#include "innodb0types.h"
#include "log0types.h"

#include <wal/wal.h>
#include <util/thread_pool.h>

#include <functional>
#include <memory>
#include <string>

/**
 * WAL backend adapter for InnoDB redo logging.
 * Wraps wal::Log to provide InnoDB-compatible interface.
 */
struct Log_wal_backend {

  /**
   * Configuration for the WAL backend.
   */
  struct Config {
    /** Number of blocks per buffer (default: derived from LOG_BUFFER_SIZE) */
    size_t m_n_blocks{4096};

    /** Block size in bytes (must match InnoDB's LOG_DATA_PAGE_SIZE) */
    size_t m_block_size{4096};

    /** Number of buffers in the pool */
    size_t m_pool_size{32};

    /** Number of I/O threads */
    size_t m_n_io_threads{2};

    /** Log directory path */
    std::string m_log_dir{"."};

    /** Log file name */
    std::string m_log_file{"ib_logfile0"};
  };

  /**
   * Constructor.
   *
   * @param[in] config Configuration for the backend.
   * @param[in] start_lsn Starting LSN for the log.
   */
  explicit Log_wal_backend(const Config &config, lsn_t start_lsn) noexcept;

  /**
   * Destructor.
   */
  ~Log_wal_backend() noexcept;

  /* Disable copy */
  Log_wal_backend(const Log_wal_backend &) = delete;
  Log_wal_backend &operator=(const Log_wal_backend &) = delete;

  /**
   * Open the log file.
   *
   * @return true on success, false on failure.
   */
  [[nodiscard]] bool open() noexcept;

  /**
   * Close the log file.
   *
   * @return true on success, false on failure.
   */
  [[nodiscard]] bool close() noexcept;

  /**
   * Append data to the log.
   *
   * @param[in] data Pointer to data to append.
   * @param[in] len Length of data in bytes.
   *
   * @return The LSN after the append, or 0 on failure.
   */
  [[nodiscard]] lsn_t append(const byte *data, ulint len) noexcept;

  /**
   * Flush the log up to the specified LSN.
   *
   * @param[in] lsn LSN to flush up to. Use IB_UINT64_T_MAX to flush all.
   * @param[in] sync_to_disk If true, also sync to disk (fdatasync).
   *
   * @return true on success, false on failure.
   */
  [[nodiscard]] bool flush(lsn_t lsn, bool sync_to_disk) noexcept;

  /**
   * Get the current LSN (end of written data).
   *
   * @return The current LSN.
   */
  [[nodiscard]] lsn_t get_lsn() const noexcept { return m_lsn.load(); }

  /**
   * Get the flushed LSN (data written to file).
   *
   * @return The flushed LSN.
   */
  [[nodiscard]] lsn_t get_flushed_lsn() const noexcept { return m_flushed_lsn.load(); }

  /**
   * Get the synced LSN (data synced to disk).
   *
   * @return The synced LSN.
   */
  [[nodiscard]] lsn_t get_synced_lsn() const noexcept { return m_synced_lsn.load(); }

  /**
   * Start background I/O processing.
   */
  void start_io() noexcept;

  /**
   * Stop background I/O processing.
   */
  void stop_io() noexcept;

  /**
   * Wait for all I/O to complete.
   */
  void wait_for_io_idle() noexcept;

private:
  /**
   * Callback for writing data to the log file.
   *
   * @param[in] iovecs I/O vectors to write.
   *
   * @return The new low water mark LSN on success, or error.
   */
  wal::Result<lsn_t> write_callback(std::span<struct iovec> iovecs) noexcept;

  /**
   * Callback for syncing the log file.
   *
   * @return true on success, or error.
   */
  wal::Result<bool> sync_callback() noexcept;

  /** Configuration */
  Config m_config;

  /** Log file descriptor */
  int m_fd{-1};

  /** Current write offset in the file */
  off_t m_write_offset{0};

  /** Current LSN (end of appended data) */
  std::atomic<lsn_t> m_lsn{0};

  /** Flushed LSN (data written to file) */
  std::atomic<lsn_t> m_flushed_lsn{0};

  /** Synced LSN (data synced to disk) */
  std::atomic<lsn_t> m_synced_lsn{0};

  /** Thread pool for I/O operations */
  std::unique_ptr<util::Thread_pool> m_thread_pool;

  /** WAL log instance */
  std::unique_ptr<wal::Log> m_wal;

  /** Whether I/O is running */
  std::atomic<bool> m_io_running{false};
};
