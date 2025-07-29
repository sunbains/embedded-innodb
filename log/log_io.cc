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

/** @file log/log_io.cc
WAL IO operations - file reading and writing operations

*******************************************************/

#include "log0log.h"
#include "log_io_uring.h"
#include "mem0mem.h"
#include "os0aio.h"
#include "srv0srv.h"

void Log::group_read_checkpoint_info(log_group_t *group, ulint field) noexcept {
  ut_ad(mutex_own(&m_mutex));

  ++m_n_log_ios;

  log_io(
    IO_request::Sync_log_read,
    false,
    Page_id(group->space_id, field / UNIV_PAGE_SIZE),
    field % UNIV_PAGE_SIZE,
    IB_FILE_BLOCK_SIZE,
    m_checkpoint_buf,
    nullptr
  );
}

void Log::group_read_log_seg(ulint type, byte *buf, log_group_t *group, lsn_t start_lsn, lsn_t end_lsn) noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto source_offset = group_calc_lsn_offset(start_lsn, group);
  auto len = (ulint)(end_lsn - start_lsn);

  ut_ad(len != 0);

  while (len > 0) {
    if ((source_offset % group->file_size) + len > group->file_size) {
      len = group->file_size - (source_offset % group->file_size);
    }

    ++m_n_log_ios;

    log_io(
      type == LOG_RECOVER ? IO_request::Sync_log_read : IO_request::Async_log_read,
      false,
      Page_id(group->space_id, source_offset / UNIV_PAGE_SIZE),
      source_offset % UNIV_PAGE_SIZE,
      len,
      buf,
      nullptr
    );

    start_lsn += len;
    buf += len;
    len = (ulint)(end_lsn - start_lsn);
  }
}

void Log::group_file_header_flush(log_group_t *group, ulint nth_file, lsn_t start_lsn) noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_a(nth_file < group->n_files);

  auto buf = group->file_header_bufs[nth_file];

  mach_write_to_4(buf + LOG_GROUP_ID, group->id);
  mach_write_to_8(buf + LOG_FILE_START_LSN, start_lsn);

  memcpy(buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, "    ", 4);

  auto dest_offset = nth_file * group->file_size;

  if (log_do_write) {
    ++m_n_log_ios;
    ++srv_os_log_pending_writes;

    log_io(
      IO_request::Sync_log_write,
      false,
      Page_id(group->space_id, dest_offset / UNIV_PAGE_SIZE),
      dest_offset % UNIV_PAGE_SIZE,
      IB_FILE_BLOCK_SIZE,
      buf,
      nullptr
    );

    --srv_os_log_pending_writes;
  }
}

void Log::group_write_buf(log_group_t *group, byte *buf, ulint len, lsn_t start_lsn, ulint new_data_offset) noexcept {
  ut_ad(mutex_own(&m_mutex));
  ut_a(len % IB_FILE_BLOCK_SIZE == 0);
  ut_a(((ulint)start_lsn) % IB_FILE_BLOCK_SIZE == 0);

  auto write_header = new_data_offset == 0;

  while (len > 0) {
    auto next_offset = group_calc_lsn_offset(start_lsn, group);

    if ((next_offset % group->file_size == LOG_FILE_HDR_SIZE) && write_header) {
      group_file_header_flush(group, next_offset / group->file_size, start_lsn);
      srv_os_log_written += IB_FILE_BLOCK_SIZE;
      ++srv_log_writes;
    }

    ulint write_len;

    if ((next_offset % group->file_size) + len > group->file_size) {
      write_len = group->file_size - (next_offset % group->file_size);
    } else {
      write_len = len;
    }

    for (ulint i = 0; i < write_len / IB_FILE_BLOCK_SIZE; i++) {
      block_store_checksum(buf + i * IB_FILE_BLOCK_SIZE);
    }

    if (log_do_write) {
      ++m_n_log_ios;
      ++srv_os_log_pending_writes;

      log_io(
        IO_request::Sync_log_write,
        false,
        Page_id(group->space_id, next_offset / UNIV_PAGE_SIZE),
        next_offset % UNIV_PAGE_SIZE,
        write_len,
        buf,
        nullptr
      );

      --srv_os_log_pending_writes;

      srv_os_log_written += write_len;
      ++srv_log_writes;
    }

    if (write_len < len) {
      start_lsn += write_len;
      len -= write_len;
      buf += write_len;
      write_header = true;
    } else {
      break;
    }
  }
}

void Log::io_complete(log_group_t *group) noexcept {
  if (uintptr_t(group) & 0x1UL) {
    group = reinterpret_cast<log_group_t *>(uintptr_t(group) - 1);

    if (srv_config.m_unix_file_flush_method != SRV_UNIX_O_DSYNC && srv_config.m_unix_file_flush_method != SRV_UNIX_NOSYNC) {
      // Use WAL I/O system flush instead of srv_fil->flush
      if (g_wal_io_system) {
        int file_index = static_cast<int>(group->space_id);
        g_wal_io_system->flush_file(file_index, true);
      }
    }

    io_complete_checkpoint();
    return;
  }

  ut_error;

  if (srv_config.m_unix_file_flush_method != SRV_UNIX_O_DSYNC && srv_config.m_unix_file_flush_method != SRV_UNIX_NOSYNC &&
      srv_config.m_flush_log_at_trx_commit != 2) {
    // Use WAL I/O system flush instead of srv_fil->flush
    if (g_wal_io_system) {
      int file_index = static_cast<int>(group->space_id);
      g_wal_io_system->flush_file(file_index, true);
    }
  }

  acquire();

  ut_a(group->n_pending_writes > 0);
  ut_a(m_n_pending_writes > 0);

  --group->n_pending_writes;
  --m_n_pending_writes;

  auto unlock = group_check_flush_completion(group);
  unlock |= sys_check_flush_completion();

  flush_do_unlocks(unlock);

  release();
}

db_err log_io(IO_request io_request, bool batched, const Page_id &page_id, ulint byte_offset, ulint len, void *buf, void *message) {
  ut_ad(len > 0);
  ut_ad(buf != nullptr);
  ut_ad(byte_offset < UNIV_PAGE_SIZE);

  static_assert((1 << UNIV_PAGE_SIZE_SHIFT) == UNIV_PAGE_SIZE, "error (1 << UNIV_PAGE_SIZE_SHIFT) != UNIV_PAGE_SIZE");

  // Check if WAL I/O system is initialized
  if (!g_wal_io_system) {
    log_err("WAL I/O system not initialized");
    return DB_ERROR;
  }

  bool is_sync_request{};

  switch (io_request) {
    case IO_request::Sync_log_read:
      is_sync_request = true;
      // fallthrough
    case IO_request::Async_log_read:
      break;

    case IO_request::Sync_log_write:
      is_sync_request = true;
      // falthrough
    case IO_request::Async_log_write:
      break;

    case IO_request::Sync_log_flush:
      is_sync_request = true;
      // fallthrough
    case IO_request::Async_log_flush:
      break;

    case IO_request::None:
    case IO_request::Sync_read:
    case IO_request::Async_read:
    case IO_request::Sync_write:
    case IO_request::Async_write:
      ut_error;
      break;
  }

  // Map space_id to WAL file index - for now, use space_id directly as file index
  // In a production system, you'd maintain a mapping between space_id and WAL file paths
  int file_index = static_cast<int>(page_id.space_id());

  // Calculate file offset
  off_t offset = (off_t(page_id.page_no()) * off_t(UNIV_PAGE_SIZE)) + byte_offset;

  // Validate alignment
  ut_a(byte_offset % IB_FILE_BLOCK_SIZE == 0);
  ut_a((len % IB_FILE_BLOCK_SIZE) == 0);

  // Perform I/O operation using WAL_IO_System
  ssize_t result = -1;

  if (is_sync_request) {
    // Synchronous operation
    switch (io_request) {
      case IO_request::Sync_log_read:
        result = g_wal_io_system->sync_read(file_index, buf, len, offset);
        break;
      case IO_request::Sync_log_write:
        result = g_wal_io_system->sync_write(file_index, buf, len, offset);
        break;
      default:
        ut_error;
        break;
    }

    if (result < 0) {
      log_err("Synchronous WAL I/O operation failed");
      return DB_ERROR;
    }

    if (static_cast<size_t>(result) != len) {
      log_err("Partial WAL I/O operation: requested ", len, " bytes, got ", result, " bytes");
      return DB_ERROR;
    }
  } else {
    // Asynchronous operation
    int op_id = -1;

    switch (io_request) {
      case IO_request::Async_log_read:
        op_id = g_wal_io_system->async_read(file_index, buf, len, offset, message);
        break;
      case IO_request::Async_log_write:
        op_id = g_wal_io_system->async_write(file_index, buf, len, offset, message);
        break;
      default:
        ut_error;
        break;
    }

    if (op_id < 0) {
      log_err("Failed to submit asynchronous WAL I/O operation");
      return DB_ERROR;
    }

    // For batched operations, we don't wait for completion here
    // The completion will be handled by the WAL I/O system's completion processing
  }

  return DB_SUCCESS;
}