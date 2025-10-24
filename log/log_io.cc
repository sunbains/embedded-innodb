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

void Log::io_complete(std::function<void ()> callback) noexcept {
  callback();
}

db_err log_io(IO_request io_request, int fd, void *buf, size_t len, off_t offset, bool batched, void *message) {
  ut_ad(len > 0);
  ut_ad(buf != nullptr);

  static_assert((1 << UNIV_PAGE_SIZE_SHIFT) == UNIV_PAGE_SIZE, "error (1 << UNIV_PAGE_SIZE_SHIFT) != UNIV_PAGE_SIZE");

  // Check if WAL I/O system is initialized
  if (g_wal_io_system == nullptr) {
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

  ut_a((len % IB_FILE_BLOCK_SIZE) == 0);
  ut_a(offset % IB_FILE_BLOCK_SIZE == 0);

  ssize_t result = -1;

  if (is_sync_request) {
    // Synchronous operation
    switch (io_request) {
      case IO_request::Sync_log_read:
        result = g_wal_io_system->sync_read(fd, buf, len, offset);
        break;
      case IO_request::Sync_log_write:
        result = g_wal_io_system->sync_write(fd, buf, len, offset);
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
        op_id = g_wal_io_system->async_read(fd, buf, len, offset, message);
        break;
      case IO_request::Async_log_write:
        op_id = g_wal_io_system->async_write(fd, buf, len, offset, message);
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
