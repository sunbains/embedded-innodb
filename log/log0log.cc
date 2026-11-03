/****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2009, Google Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

Portions of this file contain modifications contributed and copyrighted by
Google, Inc. Those modifications are gratefully acknowledged and are described
briefly in the InnoDB documentation. The contributions by Google are
incorporated with their permission, and subject to the conditions contained in
the file COPYING.Google.

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

/** @file log/log0log.cc
Database log

Created 12/9/1995 Heikki Tuuri
*******************************************************/

#include "log0log.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "fil0fil.h"
#include "srv0srv.h"

/** LSN of a new database; the WAL aligns it to a block boundary. */
constexpr lsn_t LOG_START_LSN = 8192;

Log::Log(System &sys) noexcept : m_sys(sys) {
  mutex_create(&m_mutex, IF_DEBUG("log_sys_mutex", )  Current_location());

  m_lsn = LOG_START_LSN;
  m_last_checkpoint_lsn = LOG_START_LSN;
}

Log::~Log() noexcept {
  mutex_free(&m_mutex);
}

lsn_t Log::buf_pool_get_oldest_modification() noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto lsn = m_sys.m_buf_pool->get_oldest_modification();

  if (lsn == 0) {
    lsn = m_lsn;
  }

  return lsn;
}

bool Log::peek_lsn(lsn_t *lsn) noexcept {
  if (mutex_enter_nowait(&m_mutex) == 0) {
    *lsn = m_lsn;

    release();

    return true;
  } else {
    return false;
  }
}

void Log::shutdown() noexcept {
  if (m_wal_backend != nullptr) {
    write_up_to(get_lsn(), true);

    m_wal_backend->stop_io();
    m_wal_backend.reset();
  }
}

lsn_t Log::close() noexcept {
  if (m_wal_backend && !m_pending_data.empty()) {
    const auto end_lsn = m_wal_backend->append(m_pending_data.data(), m_pending_data.size());

    if (end_lsn > 0) {
      m_lsn.store(end_lsn);
    }

    m_pending_data.clear();

    return end_lsn > 0 ? end_lsn : m_lsn.load();
  }

  return m_lsn;
}

bool Log::checkpoint(bool write_always) noexcept {
  /* No log is written before the WAL starts: recovery flushes its pages
  without moving the checkpoint, init_wal_backend() writes the first one. */
  if (!m_wal_backend) {
    return true;
  }

  std::lock_guard<std::mutex> guard(m_checkpoint_mutex);

  /* Every page modified below the oldest modification in the buffer pool
  has been written: the checkpoint can move there. Pages that are being
  written are still in the flush list. */
  acquire();

  const auto oldest = buf_pool_get_oldest_modification();
  const auto last = m_last_checkpoint_lsn;

  release();

  if (oldest < last || (oldest == last && !write_always)) {
    return true;
  }

  return write_checkpoint(oldest);
}

bool Log::write_checkpoint(lsn_t lsn) noexcept {
  /* Recovery reads the log from lsn: the log up to there must be durable,
  and so must the pages written to the tablespaces, which may still be in
  the file system cache. */
  write_up_to(lsn, true);

  m_sys.m_fil->flush_file_spaces();

  if (!LogWalBackend::write_checkpoint(wal_config(), m_next_checkpoint_no, lsn)) {
    return false;
  }

  ++m_next_checkpoint_no;

  acquire();

  m_last_checkpoint_lsn = lsn;

  release();

  m_wal_backend->discard_up_to(lsn);

  return true;
}

bool Log::read_checkpoint(lsn_t &lsn, uint64_t &no) noexcept {
  /* Under the same latches as write_checkpoint(): this used to update both
  fields with none, racing with a checkpoint by the master thread when
  called on a running system (a lost ++m_next_checkpoint_no would reuse a
  checkpoint number). m_checkpoint_mutex orders it against checkpoint(),
  m_mutex protects m_last_checkpoint_lsn for its other readers. */
  std::lock_guard<std::mutex> guard(m_checkpoint_mutex);

  if (!LogWalBackend::read_checkpoint(wal_config(), no, lsn)) {
    return false;
  }

  m_next_checkpoint_no = no + 1;

  acquire();

  m_last_checkpoint_lsn = lsn;

  release();

  return true;
}

LogWalBackend::Config Log::wal_config() const noexcept {
  LogWalBackend::Config config;

  config.m_log_dir = m_sys.m_config.m_log.m_log_dir;
  config.m_log_file = "redo.wal";
  config.m_pool_size = 4;
  config.m_n_blocks = std::max<size_t>(16, m_sys.m_config.m_log.m_buffer_size / config.m_block_size);

  return config;
}

void Log::make_checkpoint_at(lsn_t lsn, bool write_always) noexcept {
  auto &flusher = *m_sys.m_buf_pool->m_flusher;

  /* Flush the pages modified below lsn. batch() returns ULINT_UNDEFINED
  when another batch of the same type is running: wait for it and retry. */
  while (flusher.batch(BUF_FLUSH_LIST, ULINT_MAX, lsn) == ULINT_UNDEFINED) {
    flusher.wait_batch_end(BUF_FLUSH_LIST);
  }

  flusher.wait_batch_end(BUF_FLUSH_LIST);

  (void)checkpoint(write_always);
}

void Log::write_up_to(lsn_t lsn, bool sync_to_disk) noexcept {
  if (m_wal_backend) {
    (void) m_wal_backend->flush(lsn, sync_to_disk);
  }
}

void Log::init_wal_backend(const LogWalBackend::Config &config) noexcept {
  m_lsn.store(LogWalBackend::align_start_lsn(config, m_lsn.load()));

  m_wal_backend = std::make_unique<LogWalBackend>(config, m_lsn.load());

  if (!m_wal_backend->open()) {
    log_fatal("Cannot open the WAL");
  }

  m_wal_backend->start_io();

  /* The WAL continues at a block boundary, past where the log of the last
  run ended: recovery must start reading here. Nothing is dirty yet, the
  log before here is no longer needed. */
  ut_a(m_sys.m_buf_pool->get_oldest_modification() == 0);

  std::lock_guard<std::mutex> guard(m_checkpoint_mutex);

  /* Under m_checkpoint_mutex, which protects it (see read_checkpoint()). */
  if (m_next_checkpoint_no == 0) {
    m_next_checkpoint_no = 1;
  }

  if (!write_checkpoint(m_lsn.load())) {
    log_fatal("Cannot write the first checkpoint of the WAL");
  }
}

lsn_t Log::reserve_and_write_fast(byte *data, ulint len, lsn_t *start_lsn) noexcept {
  if (!m_wal_backend) {
    *start_lsn = m_lsn;
    return 0;
  }

  *start_lsn = m_wal_backend->get_lsn();

  const auto end_lsn = m_wal_backend->append(data, len);

  if (end_lsn > 0) {
    m_lsn.store(end_lsn);
  }

  return end_lsn;
}

lsn_t Log::reserve_and_open(ulint len) noexcept {
  /* The caller releases the log mutex after close(). */
  acquire();

  if (!m_wal_backend) {
    return m_lsn;
  }

  m_pending_data.clear();
  m_pending_data.reserve(len);

  return m_wal_backend->get_lsn();
}

void Log::write_low(const byte *data, ulint len) noexcept {
  if (!m_wal_backend) {
    return;
  }

  m_pending_data.insert(m_pending_data.end(), data, data + len);
}
