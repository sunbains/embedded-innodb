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

#include "log0log.h"
#include "log0recv.h"
#include "srv0srv.h"

LogCheckpoint::~LogCheckpoint() noexcept {
  rw_lock_free(&m_lock);
}

std::string LogCheckpoint::to_string() const noexcept {
  return std::format(
    "Checkpoint: next_no: {}, next_lsn: {}, last_lsn: {}, next_lsn: {},"
    " max_modified_age_async: {}, max_modified_age_sync: {}, interval: {}, max_age_async: {},"
    " max_age: {}, n_pending_writes: {}",
    m_next_no,
    m_next_lsn,
    m_last_lsn,
    m_next_lsn,
    m_max_modified_age_async,
    m_max_modified_age_sync,
    m_adm_interval,
    m_max_age_async,
    m_max_age,
    m_n_pending_writes
  );
}

ulint LogCheckpoint::prepare(Log &log) noexcept {
  ut_ad(mutex_own(&log.m_mutex));

  m_phy_checkpoint.set_no(m_next_no);
  m_phy_checkpoint.set_lsn(m_next_lsn);
  m_phy_checkpoint.set_offset(log.m_vfile.calc_lsn_offset(m_next_lsn));

  const auto data_len = sizeof(&m_phy_checkpoint - sizeof(m_phy_checkpoint.m_checksum));
  m_phy_checkpoint.set_checksum(log_calc_checksum(reinterpret_cast<const byte *>(&m_phy_checkpoint), data_len));

  return (m_next_no & 1) == 0 ? LOG_CHECKPOINT_1 : LOG_CHECKPOINT_2;
}

bool LogCheckpoint::persist(LogVfile &log_group, off_t write_offset) noexcept {
  ut_ad(write_offset == LOG_CHECKPOINT_1 || write_offset == LOG_CHECKPOINT_2);

  ++m_n_pending_writes;

  return log_group.write_checkpoint(&m_phy_checkpoint, write_offset);
}

bool LogCheckpoint::read(off_t offset) noexcept {
  ut_ad(offset == LOG_CHECKPOINT_1 || offset == LOG_CHECKPOINT_2);

  return false;
}

void LogCheckpoint::complete() noexcept {
  ut_ad(m_n_pending_writes == 0);

  ++m_next_no;
  m_last_lsn = m_next_lsn;

  rw_lock_x_unlock_gen(&m_lock, LOG_CHECKPOINT);
}

void LogCheckpoint::io_complete() noexcept {
  ut_ad(m_n_pending_writes > 0);

  --m_n_pending_writes;

  if (m_n_pending_writes == 0) {
    complete();
  }
}
