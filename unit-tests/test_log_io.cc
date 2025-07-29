/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

Unit tests for log_io.cc functions
*****************************************************************************/

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>

#include "db0err.h"
#include "log/log_io_uring.h"
#include "log0log.h"
#include "mem0mem.h"
#include "os0aio.h"
#include "page0types.h"
#include "srv0srv.h"

// Global WAL I/O system for testing
extern WAL_IO_System *g_wal_io_system;

class LogIOTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize WAL I/O system if not already initialized
    if (!g_wal_io_system) {
      wal_io_system_init();
    }

    // Create test directory
    test_dir = "/tmp/innodb_log_io_test";
    std::filesystem::create_directories(test_dir);

    // Create test log file
    test_log_file = test_dir + "/ib_logfile0";
    create_test_log_file();

    // Open the test file in WAL I/O system
    file_index = g_wal_io_system->open_wal_file(test_log_file.c_str(), test_file_size);
    ASSERT_GE(file_index, 0) << "Failed to open test log file";
  }

  void TearDown() override {
    // Close the test file
    if (file_index >= 0) {
      g_wal_io_system->close_wal_file(file_index);
    }

    // Clean up test files
    std::filesystem::remove_all(test_dir);
  }

  void create_test_log_file() {
    int fd = open(test_log_file.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    ASSERT_GE(fd, 0) << "Failed to create test log file";

    // Write some test data
    char buffer[IB_FILE_BLOCK_SIZE];
    memset(buffer, 0xAB, sizeof(buffer));

    for (size_t i = 0; i < test_file_size / IB_FILE_BLOCK_SIZE; ++i) {
      ssize_t written = write(fd, buffer, sizeof(buffer));
      ASSERT_EQ(written, sizeof(buffer)) << "Failed to write test data";
    }

    close(fd);
  }

  std::string test_dir;
  std::string test_log_file;
  int file_index = -1;
  static constexpr size_t test_file_size = 64 * 1024;  // 64KB
};

// Test the log_io function with sync read operations
TEST_F(LogIOTest, SyncReadOperation) {
  char buffer[IB_FILE_BLOCK_SIZE];
  memset(buffer, 0, sizeof(buffer));

  Page_id page_id(file_index, 0);

  db_err result = log_io(IO_request::Sync_log_read, false, page_id, 0, IB_FILE_BLOCK_SIZE, buffer, nullptr);

  EXPECT_EQ(result, DB_SUCCESS);

  // Verify that data was read (should be 0xAB pattern from test file)
  for (size_t i = 0; i < IB_FILE_BLOCK_SIZE; ++i) {
    EXPECT_EQ(static_cast<unsigned char>(buffer[i]), 0xAB);
  }
}

// Test the log_io function with sync write operations
TEST_F(LogIOTest, SyncWriteOperation) {
  char write_buffer[IB_FILE_BLOCK_SIZE];
  char read_buffer[IB_FILE_BLOCK_SIZE];

  // Prepare test pattern
  memset(write_buffer, 0xCD, sizeof(write_buffer));
  memset(read_buffer, 0, sizeof(read_buffer));

  Page_id page_id(file_index, 1);

  // Write data
  db_err write_result = log_io(IO_request::Sync_log_write, false, page_id, 0, IB_FILE_BLOCK_SIZE, write_buffer, nullptr);

  EXPECT_EQ(write_result, DB_SUCCESS);

  // Read back and verify
  db_err read_result = log_io(IO_request::Sync_log_read, false, page_id, 0, IB_FILE_BLOCK_SIZE, read_buffer, nullptr);

  EXPECT_EQ(read_result, DB_SUCCESS);

  // Verify data matches
  EXPECT_EQ(memcmp(write_buffer, read_buffer, IB_FILE_BLOCK_SIZE), 0);
}

// Test invalid parameters
TEST_F(LogIOTest, InvalidParameters) {
  char buffer[IB_FILE_BLOCK_SIZE];
  Page_id page_id(file_index, 0);

#ifdef UNIV_DEBUG
  // In debug builds, these should trigger assertions
  // Test with null buffer - this should trigger an assertion
  EXPECT_DEATH({ log_io(IO_request::Sync_log_read, false, page_id, 0, IB_FILE_BLOCK_SIZE, nullptr, nullptr); }, "buf != nullptr");

  // Test with zero length - this should trigger an assertion
  EXPECT_DEATH({ log_io(IO_request::Sync_log_read, false, page_id, 0, 0, buffer, nullptr); }, "len > 0");
#else
  // In release builds, test error handling
  db_err result = log_io(IO_request::Sync_log_read, false, page_id, 0, IB_FILE_BLOCK_SIZE, nullptr, nullptr);

  EXPECT_EQ(result, DB_ERROR);

  // Test with zero length
  result = log_io(IO_request::Sync_log_read, false, page_id, 0, 0, buffer, nullptr);

  EXPECT_EQ(result, DB_ERROR);
#endif
}

// Test byte offset alignment
TEST_F(LogIOTest, ByteOffsetAlignment) {
#ifdef NDEBUG
  char buffer[IB_FILE_BLOCK_SIZE];
  Page_id page_id(file_index, 0);

  // Test unaligned byte offset (should fail assertion in debug build)
  // In release build, the function should handle it gracefully
  db_err result = log_io(
    IO_request::Sync_log_read,
    false,
    page_id,
    123,  // Unaligned offset
    IB_FILE_BLOCK_SIZE,
    buffer,
    nullptr
  );

  // In release mode, this might succeed or fail depending on implementation
  // We just check that it doesn't crash
  EXPECT_TRUE(result == DB_SUCCESS || result == DB_ERROR);
#else
  // In debug builds, this test is skipped as it would trigger assertions
  GTEST_SKIP() << "Skipping alignment test in debug build";
#endif
}

// Test when WAL I/O system is not initialized
TEST_F(LogIOTest, WALIOSystemNotInitialized) {
  // Temporarily shutdown WAL I/O system
  WAL_IO_System *backup = g_wal_io_system;
  g_wal_io_system = nullptr;

  char buffer[IB_FILE_BLOCK_SIZE];
  Page_id page_id(0, 0);

  db_err result = log_io(IO_request::Sync_log_read, false, page_id, 0, IB_FILE_BLOCK_SIZE, buffer, nullptr);

  EXPECT_EQ(result, DB_ERROR);

  // Restore WAL I/O system
  g_wal_io_system = backup;
}

// Test different IO request types
TEST_F(LogIOTest, IORequestTypes) {
  char buffer[IB_FILE_BLOCK_SIZE];
  Page_id page_id(file_index, 0);

  // Test sync read
  db_err result = log_io(IO_request::Sync_log_read, false, page_id, 0, IB_FILE_BLOCK_SIZE, buffer, nullptr);
  EXPECT_EQ(result, DB_SUCCESS);

  // Test sync write
  result = log_io(IO_request::Sync_log_write, false, page_id, 0, IB_FILE_BLOCK_SIZE, buffer, nullptr);
  EXPECT_EQ(result, DB_SUCCESS);

  // Note: Async operations are submitted successfully but we don't wait for completion
  // in this simple test to avoid potential blocking issues

  // Test async read
  result = log_io(IO_request::Async_log_read, false, page_id, 0, IB_FILE_BLOCK_SIZE, buffer, nullptr);
  EXPECT_EQ(result, DB_SUCCESS);

  // Test async write
  result = log_io(IO_request::Async_log_write, false, page_id, 0, IB_FILE_BLOCK_SIZE, buffer, nullptr);
  EXPECT_EQ(result, DB_SUCCESS);

  // Give async operations a moment to complete and process some completions
  if (g_wal_io_system) {
    g_wal_io_system->process_completions(10, 100);  // Process up to 10 events with 100ms timeout
  }
}

// Test invalid IO request types
TEST_F(LogIOTest, InvalidIORequestTypes) {
#ifdef NDEBUG
  char buffer[IB_FILE_BLOCK_SIZE];
  Page_id page_id(file_index, 0);

  // These should trigger ut_error in debug builds
  // In release builds, behavior is undefined but shouldn't crash
  db_err result = log_io(
    IO_request::Sync_read,  // Not a log request
    false,
    page_id,
    0,
    IB_FILE_BLOCK_SIZE,
    buffer,
    nullptr
  );

  // Implementation might handle this differently
  EXPECT_TRUE(result == DB_SUCCESS || result == DB_ERROR);
#else
  // In debug builds, this test is skipped as it would trigger ut_error
  GTEST_SKIP() << "Skipping invalid request type test in debug build";
#endif
}

// Test large read/write operations
TEST_F(LogIOTest, LargeOperations) {
  // Test reading multiple blocks
  const size_t large_size = 4 * IB_FILE_BLOCK_SIZE;
  char buffer[large_size];

  Page_id page_id(file_index, 0);

  db_err result = log_io(IO_request::Sync_log_read, false, page_id, 0, large_size, buffer, nullptr);

  EXPECT_EQ(result, DB_SUCCESS);

  // Verify the data pattern
  for (size_t i = 0; i < large_size; ++i) {
    EXPECT_EQ(static_cast<unsigned char>(buffer[i]), 0xAB);
  }
}

// Test edge cases with file boundaries
TEST_F(LogIOTest, FileBoundaryOperations) {
  // Test reading at the end of file
  char buffer[IB_FILE_BLOCK_SIZE];

  // Calculate last page
  ulint last_page = (test_file_size / UNIV_PAGE_SIZE) - 1;
  Page_id page_id(file_index, last_page);

  db_err result = log_io(IO_request::Sync_log_read, false, page_id, 0, IB_FILE_BLOCK_SIZE, buffer, nullptr);

  EXPECT_EQ(result, DB_SUCCESS);
}

// Mock test for log group operations (if log_group_t is available)
class LogGroupIOTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Note: This would require proper Log system initialization
    // For now, we'll test what we can without full system setup
  }
};

// Test helper function to validate that our test setup is correct
TEST_F(LogIOTest, TestSetup) {
  EXPECT_TRUE(std::filesystem::exists(test_log_file));
  EXPECT_GE(file_index, 0);
  EXPECT_TRUE(g_wal_io_system != nullptr);
  EXPECT_TRUE(g_wal_io_system->is_file_open(file_index));
  EXPECT_EQ(g_wal_io_system->get_file_size(file_index), test_file_size);
}