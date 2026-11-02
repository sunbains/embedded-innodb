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

/** @file log/log0log.c
Database log

Created 12/9/1995 Heikki Tuuri
*******************************************************/

#include "log0log.h"
#include "buf0buf.h"
#include "mem0mem.h"
#include "srv0srv.h"

IF_DEBUG(bool log_debug_writes = false;)

/* Global log system variable */

IF_DEBUG(bool log_do_write = true;)

Log::Log(System &sys) noexcept : m_sys(sys), m_buffer(sys.m_config.m_log.m_buffer_size) {
  mutex_create(&m_mutex, IF_DEBUG("log_sys_mutex", ) IF_SYNC_DEBUG(SYNC_LOG, ) Current_location());

  m_lsn = LOG_START_LSN;

  ut_a(LOG_BUFFER_SIZE >= 16 * sizeof(LogPhyBlock));

  m_no_flush_event = os_event_create(nullptr);

  os_event_set(m_no_flush_event);

  m_one_flushed_event = os_event_create(nullptr);

  os_event_set(m_one_flushed_event);

  m_checkpoint.m_last_lsn = m_lsn;

  m_lsn = LOG_START_LSN + LogPhyBlock::HEADER_SIZE;
}

Log::~Log() noexcept {
  mutex_free(&m_mutex);
  os_event_free(m_no_flush_event);
  os_event_free(m_one_flushed_event);
}

lsn_t Log::buf_pool_get_oldest_modification() noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto lsn = m_sys.m_buf_pool->get_oldest_modification();

  if (lsn == 0) {
    lsn = m_lsn;
  }

  return lsn;
}

bool Log::calc_max_ages() noexcept {
  acquire();

  auto capacity = m_vfile.get_capacity();

  capacity = capacity - capacity / 10;

  ulint margin{};

  auto free = 4 * UNIV_PAGE_SIZE * 10 + 8 * UNIV_PAGE_SIZE;

  if (free >= static_cast<ulint>(capacity / 2)) {
    log_fatal("Redo log files are too small");
  } else {
    margin = capacity - free;
  }

  margin = std::min<ulint>(margin, m_checkpoint.m_adm_interval);

  margin = margin - margin / 10;

  m_checkpoint.m_max_modified_age_async = margin - margin / 6;
  m_checkpoint.m_max_modified_age_sync = margin - margin / 32;
  m_checkpoint.m_max_age_async = margin - margin / 32;
  m_checkpoint.m_max_age = margin;

  release();

  return true;
}

bool Log::open(ulint n_files, off_t file_size) noexcept {
  return false;
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
  if (this == nullptr) {
    return;
  }

  (void)m_vfile.close();

  os_event_free(m_no_flush_event);
  os_event_free(m_one_flushed_event);
}

lsn_t Log::close([[maybe_unused]] IbRecovery recovery) noexcept {
  if (m_wal_backend && !m_pending_data.empty()) {
    // Flush accumulated data from slow path
    auto end_lsn = m_wal_backend->append(m_pending_data.data(), m_pending_data.size());
    if (end_lsn > 0) {
      m_lsn.store(end_lsn);
    }
    m_pending_data.clear();
    return end_lsn > 0 ? end_lsn : m_lsn.load();
  }
  return m_lsn;
}

bool Log::checkpoint([[maybe_unused]] bool sync, [[maybe_unused]] bool write_always) noexcept {
  return true;
}

void Log::make_checkpoint_at([[maybe_unused]] lsn_t lsn, [[maybe_unused]] bool write_always) noexcept {
}

void Log::write_up_to([[maybe_unused]] lsn_t lsn, [[maybe_unused]] bool async, [[maybe_unused]] bool sync_to_disk) noexcept {
  if (m_wal_backend) {
    (void) m_wal_backend->flush(lsn, sync_to_disk);
  }
}

void Log::init_wal_backend(const LogWalBackend::Config &config) noexcept {
  m_wal_backend = std::make_unique<LogWalBackend>(config, m_lsn.load());
  (void) m_wal_backend->open();
  m_wal_backend->start_io();
}

lsn_t Log::reserve_and_write_fast(byte *data, ulint len, lsn_t *start_lsn) noexcept {
  if (!m_wal_backend) {
    *start_lsn = m_lsn;
    return 0;
  }

  *start_lsn = m_wal_backend->get_lsn();
  auto end_lsn = m_wal_backend->append(data, len);

  if (end_lsn > 0) {
    m_lsn.store(end_lsn);
    return end_lsn;
  }

  return 0;
}

lsn_t Log::reserve_and_open(ulint len) noexcept {
  if (!m_wal_backend) {
    return m_lsn;
  }

  // Clear pending data for slow path
  m_pending_data.clear();
  m_pending_data.reserve(len);
  m_pending_start_lsn = m_wal_backend->get_lsn();

  return m_pending_start_lsn;
}

void Log::write_low(const byte *data, ulint len) noexcept {
  if (!m_wal_backend) {
    return;
  }

  // Accumulate data for slow path
  m_pending_data.insert(m_pending_data.end(), data, data + len);
}
