/****************************************************************************
Copyright (c) 2026 Sunny Bains. All rights reserved.

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

/** @file include/srv0cfg.h
Configuration of one InnoDB instance.

Config is a plain value: the caller fills it in, and System::init() takes it
by const reference, validates it and keeps its own copy. Each sub-system is
created from its own sub-config and never sees the rest. Nothing in here
changes after System::init(); run-time state (current sizes, "file per
table" being suspended during bootstrap, ...) belongs to the sub-system that
owns it. */

#pragma once

#include "innodb0types.h"

#include <chrono>
#include <string>

/** How data files are written and the log is synced. */
enum FlushMethod : ulint {
  /** Data files through the file system cache, synced with fsync(). */
  SRV_UNIX_FSYNC = 1,

  /** A commit does not sync the log. */
  SRV_UNIX_NOSYNC,

  /** Data files bypass the file system cache. */
  SRV_UNIX_O_DIRECT
};

/** Alternatives for forced recovery. Non-zero values are intended to help
the user get a damaged database up so that he can dump intact tables and rows
with SELECT INTO OUTFILE. The database must not otherwise be used with these
options! A bigger number below means that all precautions of lower numbers
are included.
NOTE: The order below is important, the code uses <= and >= to check for
recovery levels. */
enum IbRecovery {
  IB_RECOVERY_DEFAULT,
  IB_RECOVERY_IGNORE_CORRUPT,
  /** Prevent the main thread from running: if a crash would occur in purge,
  this prevents it */
  IB_RECOVERY_NO_BACKGROUND,
  IB_RECOVERY_NO_TRX_UNDO,
  /** Do not look at undo logs when starting the database: InnoDB will treat
  even incomplete transactions as committed */
  IB_RECOVERY_NO_UNDO_LOG_SCAN,
  IB_RECOVERY_NO_LOG_REDO
};

/** Asynchronous I/O. */
struct AioConfig {
  ulint m_n_read_threads{4};
  ulint m_n_write_threads{4};
};

/** Tablespace files. */
struct FilConfig {
  /** Directory holding the system tablespace and the .ibd files. */
  std::string m_data_home{"./"};

  /** Where the temporary files of sorts go, unnamed: the data home if empty. */
  std::string m_tmp_dir{};

  /** Create each user table in its own .ibd file. */
  bool m_file_per_table{true};

  ulint m_max_n_open_files{1024};

  FlushMethod m_flush_method{SRV_UNIX_FSYNC};
};

/** Buffer pool and page flushing. */
struct BufPoolConfig {
  ulint m_size{8 * 1024 * 1024};

  /** Percentage of the LRU list kept as the "old" sublist. */
  ulint m_lru_old_blocks_pct{3 * 100 / 8};

  /** Milliseconds a block must stay in the old sublist before an access
  moves it to the young end. */
  ulint m_lru_block_access_recency{};

  ulint m_read_ahead_threshold{56};

  /** Flushing is throttled to this many page writes per second. */
  ulint m_io_capacity{200};

  bool m_adaptive_flushing{true};

  ulint m_max_modified_pct{90};

  bool m_use_checksums{true};
};

/** Doublewrite buffer. */
struct DblwrConfig {
  bool m_enabled{true};
};

/** Redo log. */
struct LogConfig {
  /** Directory of the WAL (redo.wal) and its checkpoint (redo.ckpt). */
  std::string m_log_dir{"."};

  /** Redo in bytes that the checkpoint may trail the current LSN by: the
  page flushing keeps up with the redo at this distance. It bounds the redo
  that recovery reads and the disk space of the WAL. */
  ulint m_checkpoint_age{64 * 1024 * 1024};

  /** Size of the log buffer in bytes. */
  ulint m_buffer_size{384 * 1024};

  /** 0: write and flush once a second, 1: flush at every commit,
  2: write at commit, flush once a second. */
  ulint m_flush_at_trx_commit{1};
};

/** Crash recovery. */
struct RecvConfig {
  IbRecovery m_force_recovery{IB_RECOVERY_DEFAULT};

  /** The most memory the records read but not yet applied take, then a batch of them is applied:
  0 is the buffer pool but for the frames the batch reads pages into. */
  ulint m_max_memory{};
};

/** Lock manager. */
struct LockConfig {
  ulint m_table_size{};

  std::chrono::seconds m_wait_timeout{60};

  /** Roll back the whole transaction, not just the statement, on a lock
  wait timeout. */
  bool m_rollback_on_timeout{true};
};

/** Transactions and purge. */
struct TrxConfig {
  /** Maximum allowed purge history length, 0 means no limit. */
  ulint m_max_purge_lag{};

  /** The node id of this instance, below ut::HLC_MAX_NODES: stored at creation,
  and any later start with another one is refused. */
  uint32_t m_node_id{};

  /** The epoch of the clock, below ut::HLC_MAX_EPOCHS, at least: the stored
  one is kept if it is larger. */
  uint64_t m_epoch{1};

  /** Start the clock at this value, if it is above the others: UNIX microseconds. */
  uint64_t m_hlc_start{};
};

/** An index implementation, see IndexAm. The catalog says which one an index has: DICT_BLINK. */
enum class IndexAmKind : uint8_t {
  /** The B-tree of btr0btr.cc. */
  BTREE,

  /** The Lehman & Yao B-link tree of idx0blink.cc. */
  BLINK,

  /** In memory, for tests, idx0mem.cc: the system tables are B-link, they have to survive a restart. */
  MEM
};

/** Where the fields go that a record is too big for, see BlobStore. */
enum class BlobStoreKind : uint8_t {
  /** In pages of the index's segment, blob0inline.cc. */
  INLINE,

  /** In memory, for tests, blob0mem.cc: nothing survives a restart. */
  MEM
};

/** Data dictionary. */
struct DictConfig {
  uint64_t m_stats_sample_pages{8};

  /** The implementation of the indexes created from now on, and of the system tables of a new database. */
  IndexAmKind m_index_am{IndexAmKind::BLINK};

  /** The store of the fields the records are too big for. */
  BlobStoreKind m_blob_store{BlobStoreKind::INLINE};

  /** On a B-link tree: inserts that find a page owned queue on it, for the owner to do them in a batch.
  Off, they wait for the page each for itself. */
  bool m_insert_batch{true};
};

/** Server threads and shutdown. */
struct SrvConfig {
  /** Maximum number of threads that can wait inside InnoDB, 0 means derive
  it from the buffer pool size. */
  ulint m_max_n_threads{};

  /** Print the status output periodically. */
  bool m_status{};

  /** Print extra information during startup and shutdown. */
  bool m_verbose_log{true};

};

struct Config {
  AioConfig m_aio{};
  FilConfig m_fil{};
  BufPoolConfig m_buf_pool{};
  DblwrConfig m_dblwr{};
  LogConfig m_log{};
  RecvConfig m_recv{};
  LockConfig m_lock{};
  TrxConfig m_trx{};
  DictConfig m_dict{};
  SrvConfig m_srv{};

  /** Check that the values are consistent with each other, eg. the
  checkpoint age, and fill in the derived ones (e.g.
  m_srv.m_max_n_threads).
  @return DB_SUCCESS, or DB_INVALID_INPUT and a message is logged. */
  [[nodiscard]] DbErr normalize() noexcept;
};
