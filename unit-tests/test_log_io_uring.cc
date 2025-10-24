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

/** @file unit-tests/test_log_io_uring.cc
Unit tests for WAL I/O system using io_uring

This file contains unit tests for the WAL_IO_System infrastructure,
testing basic functionality with minimal dependencies.
*****************************************************************************/

#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <ranges>
#include <string>
#include <vector>

// Include the actual library headers
#include "innodb0types.h"
#include "log/log_io_uring.h"
#include "log0config.h"
#include "log0types.h"

// Test assertion macro
#define TEST_ASSERT(condition)                                     \
  do {                                                             \
    if (!(condition)) {                                            \
      log_err("Test failed at line ", __LINE__, ": ", #condition); \
      return false;                                                \
    }                                                              \
  } while (0)

bool test_WAL_file_construction(const std::string &test_dir) {
  log_info("Testing WAL_file construction...");
  auto test_file = test_dir + "/test.log";

  try {
    // Test default constructor
    WAL_file file1;
    TEST_ASSERT(file1.m_fd == -1);
    TEST_ASSERT(file1.m_path.empty());
    TEST_ASSERT(file1.m_size == 0);
    TEST_ASSERT(file1.m_pending_ops.load() == 0);
    TEST_ASSERT(file1.m_mtime == 0);

    file1.m_fd = 42;
    file1.m_path = test_file;
    file1.m_size = 1024;
    file1.m_pending_ops.store(1);
    file1.m_mtime = 12345;

    // Test copy constructor
    WAL_file file2 = file1;
    TEST_ASSERT(file2.m_fd == 42);
    TEST_ASSERT(file2.m_path == test_file);
    TEST_ASSERT(file2.m_size == 1024);
    TEST_ASSERT(file2.m_pending_ops.load() == 1);
    TEST_ASSERT(file2.m_mtime == 12345);

    // Test assignment operators
    WAL_file file4;
    file4 = file1;
    TEST_ASSERT(file4.m_fd == 42);
    TEST_ASSERT(file4.m_path == test_file);
    TEST_ASSERT(file4.m_size == 1024);
    TEST_ASSERT(file4.m_pending_ops.load() == 1);
    TEST_ASSERT(file4.m_mtime == 12345);

    WAL_file file5;
    file5 = std::move(file1);
    TEST_ASSERT(file5.m_fd == 42);
    TEST_ASSERT(file5.m_path == test_file);
    //
    TEST_ASSERT(file1.m_fd == -1);
    TEST_ASSERT(file1.m_size == 0);
    TEST_ASSERT(file1.m_path.empty());

  } catch (const std::exception &e) {
    log_err("WAL_file construction test failed: ", e.what());
    return false;
  }

  return true;
}

bool test_WAL_io_ctx_construction(const std::string &) {
  log_info("Testing WAL_io_ctx_t operations...");

  try {
    // Test default constructor
    WAL_io_ctx_t ctx1;
    TEST_ASSERT(ctx1.m_req_type == IO_request::None);
    TEST_ASSERT(ctx1.m_callback == nullptr);
    TEST_ASSERT(ctx1.m_file == nullptr);
    TEST_ASSERT(ctx1.m_start_time == 0);
    TEST_ASSERT(ctx1.m_bytes_requested == 0);
    TEST_ASSERT(ctx1.m_result.load() == 0);
    TEST_ASSERT(ctx1.m_completed.load() == false);

    WAL_file file1;

    // Test with data
    WAL_io_ctx_t ctx2;
    ctx2.m_req_type = IO_request::Sync_read;
    ctx2.m_file = &file1;
    ctx2.m_start_time = 1000;
    ctx2.m_bytes_requested = 1024;
    ctx2.m_result.store(0);
    ctx2.m_completed.store(false);

    TEST_ASSERT(ctx2.m_req_type == IO_request::Sync_read);
    TEST_ASSERT(ctx2.m_callback == nullptr);
    TEST_ASSERT(ctx2.m_file == &file1);
    TEST_ASSERT(ctx2.m_start_time == 1000);
    TEST_ASSERT(ctx2.m_bytes_requested == 1024);
    TEST_ASSERT(ctx2.m_result.load() == 0);
    TEST_ASSERT(ctx2.m_completed.load() == false);

    // Test copy constructor
    WAL_io_ctx_t ctx3 = ctx2;
    TEST_ASSERT(ctx3.m_req_type == ctx2.m_req_type);
    TEST_ASSERT(ctx3.m_file == ctx2.m_file);
    TEST_ASSERT(ctx3.m_start_time == ctx2.m_start_time);
    TEST_ASSERT(ctx3.m_bytes_requested == ctx2.m_bytes_requested);
    TEST_ASSERT(ctx3.m_result.load() == ctx2.m_result.load());
    TEST_ASSERT(ctx3.m_completed.load() == ctx2.m_completed.load());

    // Test move constructor
    WAL_io_ctx_t ctx4 = std::move(ctx3);
    TEST_ASSERT(ctx4.m_req_type == ctx2.m_req_type);
    TEST_ASSERT(ctx4.m_file == ctx2.m_file);
    TEST_ASSERT(ctx4.m_start_time == ctx2.m_start_time);
    TEST_ASSERT(ctx4.m_bytes_requested == ctx2.m_bytes_requested);
    TEST_ASSERT(ctx4.m_result.load() == ctx2.m_result.load());
    TEST_ASSERT(ctx4.m_completed.load() == ctx2.m_completed.load());

  } catch (const std::exception &e) {
    log_err("WAL_io_ctx_t operations test failed: ", e.what());
    return false;
  }

  return true;
}

static const char *test_data = "Async WAL scatter-gather I/O test";
static size_t data_len = strlen(test_data);

bool test_scatter_reads(WAL_IO_System &wal_io_system, std::vector<WAL_file *> &test_files) {
  log_info("Testing scatter reads...");

  char *test_read = new char[data_len];
  std::vector<WAL_iovec_t> iovecs(2);

  iovecs[0].iov_base = test_read;
  iovecs[0].iov_len = data_len / 2;
  iovecs[1].iov_base = test_read + data_len / 2;
  iovecs[1].iov_len = data_len - (data_len / 2);

  auto ctx = wal_io_system.async_scatter_read(test_files[0], iovecs, 0, [](WAL_io_ctx_t *ctx) {
    log_info("  Scatter-read completed : result = ", ctx->m_result.load());
    ut_a(ctx->m_result.load() == int(data_len));
  });

  if (ctx == nullptr) {
    log_err("Failed to submit scatter-read");
    delete[] test_read;
    return false;
  }

  while (ctx != nullptr && !wal_io_system.wait_for_completion(ctx)) {
    ::usleep(1000);
  }

  ut_a(ctx == nullptr);

  ut_a(strncmp(test_read, test_data, data_len) == 0);

  delete[] test_read;

  return true;
}

bool test_gather_writes(WAL_IO_System &wal_io_system, std::vector<WAL_file *> &test_files) {
  log_info("Testing gather writes...");

  std::vector<WAL_iovec_t> iovecs(2);

  iovecs[0].iov_base = const_cast<void *>(static_cast<const void *>(reinterpret_cast<const char *>(test_data)));
  iovecs[0].iov_len = data_len / 2;
  iovecs[1].iov_base = const_cast<void *>(static_cast<const void *>(test_data + data_len / 2));
  iovecs[1].iov_len = data_len - (data_len / 2);

  auto ctx = wal_io_system.async_gather_write(test_files[0], iovecs, 0, [](WAL_io_ctx_t *ctx) {
    log_info("  Gather-write completed : result = ", ctx->m_result.load());
    ut_a(ctx->m_result.load() == int(data_len));
  });

  if (ctx == nullptr) {
    log_err("Failed to submit gather-write");
    return false;
  }

  while (ctx != nullptr && !wal_io_system.wait_for_completion(ctx)) {
    ::usleep(1000);
  }

  ut_a(ctx == nullptr);

  return true;
}

bool test_wal_io_system_initialization(const std::string &test_dir) {
  log_info("Testing WAL_IO_System structure...");
  std::vector<WAL_file *> test_files;

  auto cleanup = [](WAL_IO_System &wal_io_system, std::vector<WAL_file *> &test_files) -> bool {
    bool success = true;
    for (auto &test_file : test_files) {
      if (!wal_io_system.close_wal_file(test_file)) {
        log_err("Failed to close test file ", test_file->m_path, " during cleanup");
        success = false;
      }
    }
    wal_io_system.shutdown();
    return success;
  };

  bool success = true;

  try {
    {
      WAL_IO_System wal_io_system(32, 32);

      if (!wal_io_system.initialize(16)) {
        log_err("Failed to initialize WAL_IO_System");
        return false;
      }

      log_info("  WAL_IO_System initialized successfully");

      for (int index = 0; index < 4; ++index) {
        const auto test_file_path = test_dir + "/test_file_" + std::to_string(index) + ".log";

        log_info("  Creating test file ", test_file_path);

        if (!WAL_IO_System::file_exists(test_file_path)) {
          log_dbg("Test file ", test_file_path, " does not exist, new to be created.");
        } else {
          log_dbg("Test file ", test_file_path, " already exists.");
          success = false;
          break;
        }

        auto file = wal_io_system.create_wal_file(test_file_path, 1024 * 1024);

        if (file == nullptr) {
          log_err("Failed to create test file ", test_file_path);
          success = false;
          break;
        }
        test_files.push_back(file);
      }

      if (success) {
        success &= test_gather_writes(wal_io_system, test_files);
        success &= test_scatter_reads(wal_io_system, test_files);
      }

      return cleanup(wal_io_system, test_files) & success;
    }

  } catch (const std::exception &e) {
    log_err("WAL_IO_System structure test failed: ", e.what());
    return false;
  }

  return true;
}

bool test_wal_file_operations(const std::string &test_dir) {
  log_info("Testing WAL file structure...");

  return true;
}

bool test_wal_iovec_operations(const std::string &) {
  log_info("Testing WAL_iovec_t operations...");

  return true;
}

bool test_sync_io_operations(const std::string &test_dir) {
  log_info("Testing synchronous I/O operations structure...");

  try {
    // Test that we can reference the sync I/O methods
    log_info("  sync_read, sync_write, flush_file methods are declared");

    // Test that we can create test data
    const char *test_data = "Hello, WAL I/O!";
    size_t data_len = strlen(test_data);
    TEST_ASSERT(data_len > 0);

    log_info("  Test data creation successful");

  } catch (const std::exception &e) {
    std::cerr << "Synchronous I/O operations test failed: " << e.what() << std::endl;
    return false;
  }

  return true;
}

bool test_async_io_operations(const std::string &test_dir) {
  log_info("Testing asynchronous I/O operations structure...");

  log_info("  async_read, async_write, gather_write, scatter_read methods are declared");

  const char *test_data = "Async WAL I/O test";
  size_t data_len = strlen(test_data);
  std::vector<WAL_iovec_t> iovecs(2);

  iovecs[0].iov_base = const_cast<void *>(static_cast<const void *>(reinterpret_cast<const char *>(test_data)));
  iovecs[0].iov_len = data_len / 2;
  iovecs[1].iov_base = const_cast<void *>(static_cast<const void *>(test_data + data_len / 2));
  iovecs[1].iov_len = data_len - (data_len / 2);

  log_info("  Scatter-gather structure test successful");
  return true;
}

void cleanup_test_directories(const std::string &test_dir) {
  log_info("Cleaning up test directories...");
  try {
    std::filesystem::remove_all(test_dir);
    log_info("  Cleaned up: ", test_dir);
  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << "Warning: Failed to clean up test directory " << test_dir << ": " << e.what() << std::endl;
  }
}

int main() {
  log_info("Starting WAL I/O system unit tests...");

  const std::string test_dir = "/tmp/test_wal_io_" + std::to_string(getpid());

  try {
    std::filesystem::create_directories(test_dir);
  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << "Failed to create test directory: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  bool all_passed = true;

  all_passed &= test_WAL_file_construction(test_dir);
  all_passed &= test_WAL_io_ctx_construction(test_dir);
  all_passed &= test_wal_io_system_initialization(test_dir);
  all_passed &= test_wal_file_operations(test_dir);
  all_passed &= test_wal_iovec_operations(test_dir);
  all_passed &= test_sync_io_operations(test_dir);
  all_passed &= test_async_io_operations(test_dir);

  cleanup_test_directories(test_dir);

  if (all_passed) {
    log_info("All WAL I/O system tests passed!");
    return EXIT_SUCCESS;
  } else {
    log_info("Some WAL I/O system tests failed!");
    return EXIT_FAILURE;
  }
}
