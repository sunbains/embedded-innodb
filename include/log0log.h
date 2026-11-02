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

#include "srv0sys.h"

#include "innodb0types.h"
#include "log0wal.h"
#include "srv0srv.h"
#include "sync0sync.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

/** The redo log. Mini-transactions append their log to the WAL (redo.wal),
whose LSN counts log bytes. A checkpoint (redo.ckpt) says where recovery
starts reading: every page modified below it is in the tablespaces. */
struct Log {
  /** The instance this sub-system belongs to. */
  System &m_sys;

  explicit Log(System &sys) noexcept;

  ~Log() noexcept;

  /** Acquire the log mutex. */
  void acquire() const noexcept {
    ut_ad(!mutex_own(&m_mutex));
    mutex_enter(&m_mutex);
  }

  /** Releases the log mutex. */
  void release() const noexcept {
    ut_ad(mutex_own(&m_mutex));
    mutex_exit(&m_mutex);
  }

  /** @return the current LSN: the end of the log appended so far. */
  [[nodiscard]] lsn_t get_lsn() const noexcept { return m_lsn.load(); }

  /**
   * Peeks the current LSN.
   *
   * @param[out] lsn            The current LSN, if the function returns true.
   *
   * @return true on success, false if the log mutex was taken.
   */
  [[nodiscard]] bool peek_lsn(lsn_t *lsn) noexcept;

  /**
   * Writes the log up to lsn to the WAL file.
   *
   * @param[in] lsn             Write up to this LSN; IB_UINT64_T_MAX: all.
   * @param[in] sync_to_disk    Also make it durable.
   */
  void write_up_to(lsn_t lsn, bool sync_to_disk) noexcept;

  /** Write the whole log to the WAL, and sync it if flush is set. */
  void buffer_sync_in_background(bool flush) noexcept { write_up_to(get_lsn(), flush); }

  /** Write and sync the whole log to the WAL. */
  void buffer_flush_to_disk() noexcept { write_up_to(get_lsn(), true); }

  /**
   * Makes a checkpoint at the oldest modification in the buffer pool. Does not
   * flush pages.
   *
   * @param[in] write_always    Write it even if it would not move.
   *
   * @return true on success.
   */
  [[nodiscard]] bool checkpoint(bool write_always) noexcept;

  /**
   * Flushes the pages modified below lsn and makes a checkpoint.
   *
   * @param[in] lsn             Flush up to here; IB_UINT64_T_MAX: all.
   * @param[in] write_always    Write the checkpoint even if it would not move.
   */
  void make_checkpoint_at(lsn_t lsn, bool write_always) noexcept;

  /**
   * Writes a checkpoint at lsn. The log up to lsn is made durable and the
   * tablespaces are synced first: every page modified below lsn must be
   * written. Then the WAL before lsn gives its space back.
   *
   * @param[in] lsn             Recovery starts reading the log here.
   *
   * @return true if the checkpoint was written.
   */
  [[nodiscard]] bool write_checkpoint(lsn_t lsn) noexcept;

  /**
   * Reads the latest checkpoint and sets the number of the next one.
   *
   * @param[out] lsn            Checkpoint LSN.
   * @param[out] no             Checkpoint number.
   *
   * @return false if there is no checkpoint file: a database created before
   *  the WAL kept checkpoints.
   */
  [[nodiscard]] bool read_checkpoint(lsn_t &lsn, uint64_t &no) noexcept;

  /** @return the oldest modification LSN in the buffer pool, or m_lsn if no page is dirty. */
  [[nodiscard]] lsn_t buf_pool_get_oldest_modification() noexcept;

  /** @return the configuration of the WAL: its directory, file and buffers. */
  [[nodiscard]] LogWalBackend::Config wal_config() const noexcept;

  /**
   * Initialize the WAL backend and write a checkpoint at its start LSN. The
   * WAL starts at a block boundary, past the end of the log recovery read:
   * the checkpoint makes the next recovery read from there. The buffer pool
   * must be clean, the log before the start LSN is then no longer needed.
   *
   * @param[in] config Configuration for the WAL backend.
   */
  void init_wal_backend(const LogWalBackend::Config &config) noexcept;

  /** Writes and syncs the log and stops the WAL. */
  void shutdown() noexcept;

  /**
   * Appends the log of a mini-transaction that fits in one block of its log
   * buffer. The caller holds the log mutex.
   *
   * @param[in] data            The log.
   * @param[in] len             Its length.
   * @param[out] start_lsn      LSN of the start of the log.
   *
   * @return LSN of the end of the log, 0 if the WAL is not started.
   */
  [[nodiscard]] lsn_t reserve_and_write_fast(byte *data, ulint len, lsn_t *start_lsn) noexcept;

  /**
   * Starts appending the log of a mini-transaction in pieces: acquires the
   * log mutex, which the caller releases after close().
   *
   * @param[in] len             Length of the log.
   *
   * @return LSN of the start of the log.
   */
  [[nodiscard]] lsn_t reserve_and_open(ulint len) noexcept;

  /**
   * Adds a piece of the log that reserve_and_open() started.
   *
   * @param[in] data            The piece.
   * @param[in] len             Its length.
   */
  void write_low(const byte *data, ulint len) noexcept;

  /**
   * Appends the log that reserve_and_open() started.
   *
   * @return LSN of the end of the log.
   */
  [[nodiscard]] lsn_t close() noexcept;

  /** Current log sequence number. */
  std::atomic<lsn_t> m_lsn{};

  /** Mutex protecting the log */
  mutable Mutex m_mutex{};

  /** LSN of the last checkpoint, protected by m_mutex. */
  lsn_t m_last_checkpoint_lsn{};

  /** Number of the next checkpoint. */
  uint64_t m_next_checkpoint_no{};

  /** Serializes checkpoints. */
  std::mutex m_checkpoint_mutex{};

  /** The WAL, created when the log LSN is known: see init_wal_backend(). */
  std::unique_ptr<LogWalBackend> m_wal_backend{};

  /** Log of the mini-transaction being appended in pieces. */
  std::vector<byte> m_pending_data{};
};
