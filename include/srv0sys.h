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

/** @file include/srv0sys.h
System: one InnoDB instance and the sub-systems it is made of.

The embedding application owns the System:

  Config config;
  config.m_buf_pool.m_size = 64 * 1024 * 1024;

  System sys;

  if (auto err = sys.init(config); err != DB_SUCCESS) { ... }
  if (auto err = sys.startup(); err != DB_SUCCESS) { ... }
  ...
  sys.shutdown(IB_SHUTDOWN_NORMAL);

There are no global variables. Every sub-system is created by System::init()
from its sub-config and embeds a reference to the System it belongs to
(m_sys), through which it reaches its siblings. Code that isn't a member of a
sub-system takes a System& (or the sub-system it needs) as a parameter. */

#pragma once

#include "srv0cfg.h"

#include <memory>
#include <vector>

struct Aio;
struct Btree;
struct BufPool;
struct Dblwr;
struct Dict;
struct Fil;
struct Fsp;
struct LockSys;
struct Log;
struct Parser;
struct RecvSys;
struct RowInsert;
struct RowSel;
struct RowUndo;
struct RowUpdate;
struct RowVers;
struct Srv;
struct TrxSys;
struct Undo;
struct IoHandler;

class System {
 public:
  System() noexcept;
  ~System() noexcept;

  System(const System &) = delete;
  System(System &&) = delete;
  System &operator=(const System &) = delete;
  System &operator=(System &&) = delete;

  /** Validate the config and create every sub-system from it, in
  dependency order. Nothing touches the disk yet.
  @param[in] config             Instance configuration, copied.
  @return DB_SUCCESS or error code; on error, what was created is
  destroyed again and init() may be retried. */
  [[nodiscard]] DbErr init(const Config &config) noexcept;

  /** Open or create the tablespaces and redo log, run crash recovery and
  start the background threads.
  @return DB_SUCCESS or error code. */
  [[nodiscard]] DbErr startup() noexcept;

  /** Stop the background threads, flush, and destroy the sub-systems in
  reverse creation order.
  @param[in] shutdown           How much work to do before stopping.
  @return DB_SUCCESS or error code. */
  [[nodiscard]] DbErr shutdown(ShutdownMode shutdown) noexcept;

  /** The configuration this instance was initialized with, normalized by
  init(). Read only after init(). */
  Config m_config{};

  /** Sub-systems, declared in creation order. Null before init() and after
  shutdown(). */
  std::unique_ptr<Aio> m_aio;
  std::unique_ptr<Fil> m_fil;
  std::unique_ptr<BufPool> m_buf_pool;
  std::unique_ptr<Log> m_log;
  std::unique_ptr<RecvSys> m_recv;
  std::unique_ptr<Fsp> m_fsp;
  std::unique_ptr<TrxSys> m_trx_sys;
  std::unique_ptr<LockSys> m_lock_sys;
  std::unique_ptr<Btree> m_btree;
  std::unique_ptr<Undo> m_undo;
  std::unique_ptr<Dict> m_dict;
  std::unique_ptr<Dblwr> m_dblwr;
  std::unique_ptr<RowUpdate> m_row_upd;
  std::unique_ptr<RowInsert> m_row_ins;
  std::unique_ptr<RowSel> m_row_sel;
  std::unique_ptr<RowUndo> m_row_undo;
  std::unique_ptr<RowVers> m_row_vers;

  /** The SQL parser, used for the internal SQL of the dictionary. */
  std::unique_ptr<Parser> m_parser;

  /** Server threads (master, lock timeout, monitors), their state and
  the status counters. */
  std::unique_ptr<Srv> m_srv;

 private:
  /** Abort a failed startup(): shut down what startup() created.
  @param[in] err                Error that caused the abort. */
  void startup_abort(DbErr err) noexcept;

  /** Destroy the sub-systems, in reverse creation order. */
  void destroy() noexcept;

  /** Start writing redo to the WAL, from the log's current LSN. Called once
  that LSN is known: after creating a database or after reading the
  checkpoint of an existing one. */
  void start_wal() noexcept;

  /** Arguments of the i/o handler threads, one per Aio segment. */
  std::vector<IoHandler> m_io_handlers;
};
