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

/** @file log/log_group.cc
Log group management

*******************************************************/

#include "log0log.h"
#include "mem0mem.h"

constexpr ulint LOG_UNLOCK_NONE_FLUSHED_LOCK = 1;

void Log::group_init(ulint id, ulint n_files, off_t file_size, space_id_t space_id) noexcept {
  auto group = static_cast<log_group_t *>(mem_alloc(sizeof(log_group_t)));

  new (group) log_group_t();

  group->id = id;
  group->n_files = n_files;
  group->file_size = file_size;
  group->space_id = space_id;
  group->state = LOG_GROUP_OK;
  group->lsn = LOG_START_LSN;
  group->lsn_offset = LOG_FILE_HDR_SIZE;
  group->n_pending_writes = 0;

  group->file_header_bufs_ptr = static_cast<byte **>(mem_alloc(sizeof(byte *) * n_files));
  group->file_header_bufs = static_cast<byte **>(mem_alloc(sizeof(byte *) * n_files));

  for (ulint i = 0; i < n_files; i++) {
    group->file_header_bufs_ptr[i] = static_cast<byte *>(mem_alloc(LOG_FILE_HDR_SIZE + IB_FILE_BLOCK_SIZE));
    group->file_header_bufs[i] = static_cast<byte *>(ut_align(group->file_header_bufs_ptr[i], IB_FILE_BLOCK_SIZE));
    memset(*(group->file_header_bufs + i), '\0', LOG_FILE_HDR_SIZE);
  }

  group->checkpoint_buf_ptr = static_cast<byte *>(mem_alloc(2 * IB_FILE_BLOCK_SIZE));
  group->checkpoint_buf = static_cast<byte *>(ut_align(group->checkpoint_buf_ptr, IB_FILE_BLOCK_SIZE));
  memset(group->checkpoint_buf, '\0', IB_FILE_BLOCK_SIZE);

  UT_LIST_ADD_LAST(m_log_groups, group);

  ut_a(calc_max_ages());
}

void Log::group_set_fields(log_group_t *group, lsn_t lsn) noexcept {
  group->lsn_offset = group_calc_lsn_offset(lsn, group);
  group->lsn = lsn;
}

ulint Log::group_get_capacity(const log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  return (group->file_size - LOG_FILE_HDR_SIZE) * group->n_files;
}

ulint Log::group_calc_size_offset(ulint offset, const log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  return offset - LOG_FILE_HDR_SIZE * (1 + offset / group->file_size);
}

uint64_t Log::group_calc_real_offset(ulint offset, const log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  return offset + LOG_FILE_HDR_SIZE * (1 + offset / (group->file_size - LOG_FILE_HDR_SIZE));
}

uint64_t Log::group_calc_lsn_offset(lsn_t lsn, const log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto gr_lsn = group->lsn;
  auto gr_lsn_size_offset = (int64_t)group_calc_size_offset(group->lsn_offset, group);
  auto group_size = (int64_t)group_get_capacity(group);

  int64_t difference;

  if (lsn >= gr_lsn) {
    difference = (int64_t)(lsn - gr_lsn);
  } else {
    difference = (int64_t)(gr_lsn - lsn);
    difference = difference % group_size;
    difference = group_size - difference;
  }

  auto offset = (gr_lsn_size_offset + difference) % group_size;

  ut_a(offset < (((int64_t)1) << 32));

  return group_calc_real_offset((ulint)offset, group);
}

ulint Log::group_check_flush_completion(log_group_t *group) noexcept {
  ut_ad(mutex_own(&m_mutex));

  if (!m_one_flushed && group->n_pending_writes == 0) {
    m_written_to_some_lsn = m_write_lsn;
    m_one_flushed = true;

    return LOG_UNLOCK_NONE_FLUSHED_LOCK;
  }

  return 0;
}

void Log::group_close(log_group_t *group) noexcept {
  for (ulint i = 0; i < group->n_files; ++i) {
    mem_free(group->file_header_bufs_ptr[i]);
  }

  mem_free(group->file_header_bufs);
  mem_free(group->file_header_bufs_ptr);
  mem_free(group->checkpoint_buf_ptr);
  mem_free(group);
}
