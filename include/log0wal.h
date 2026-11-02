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

#include <atomic>
#include <functional>
#include <memory>
#include <string>

/**
 * WAL backend adapter for InnoDB redo logging.
 * Wraps wal::Log to provide InnoDB-compatible interface.
 */
struct LogWalBackend {

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

    /** Log directory path */
    std::string m_log_dir{"."};

    /** Log file name */
    std::string m_log_file{"redo.wal"};

    /** Size of the WAL file in bytes; it is reused circularly. */
    off_t m_max_file_size{64 * 1024 * 1024};
  };

  /**
   * Constructor.
   *
   * @param[in] config Configuration for the backend.
   * @param[in] start_lsn Starting LSN for the log.
   */
  explicit LogWalBackend(const Config &config, lsn_t start_lsn) noexcept;

  /**
   * WAL-CPP requires the log to start at a block boundary: a buffer created at
   * an LSN inside a block miscounts the bytes of that partial block.
   *
   * @param[in] config          WAL configuration.
   * @param[in] lsn             Desired start LSN.
   *
   * @return the first block-aligned LSN >= lsn.
   */
  [[nodiscard]] static lsn_t align_start_lsn(const Config &config, lsn_t lsn) noexcept;

  /**
   * Destructor.
   */
  ~LogWalBackend() noexcept;

  /* Disable copy */
  LogWalBackend(const LogWalBackend &) = delete;
  LogWalBackend &operator=(const LogWalBackend &) = delete;

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
  /** The WAL-CPP state. WAL-CPP's headers are only included by the
  implementation: they define logging macros that clash with the engine's. */
  struct Impl;

  /** Configuration */
  Config m_config;

  /** Current LSN (end of appended data) */
  std::atomic<lsn_t> m_lsn{0};

  /** Flushed LSN (data written to file) */
  std::atomic<lsn_t> m_flushed_lsn{0};

  /** Synced LSN (data synced to disk) */
  std::atomic<lsn_t> m_synced_lsn{0};

  /** Whether I/O is running */
  std::atomic<bool> m_io_running{false};

  std::unique_ptr<Impl> m_impl;
};
