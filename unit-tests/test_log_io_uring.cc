/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

Unit tests for log/log_io_uring.cc functions
*****************************************************************************/

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <thread>

#include "db0err.h"
#include "log/log_io_uring.h"
#include "mem0mem.h"
#include "os0aio.h"
#include "page0types.h"
#include "srv0srv.h"

extern WAL_IO_System *g_wal_io_system;

class WALIOSystemTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir = "/tmp/innodb_wal_io_test";
    std::filesystem::create_directories(test_dir);

    test_log_file = test_dir + "/ib_logfile0";
    test_log_file2 = test_dir + "/ib_logfile1";

    create_test_log_file(test_log_file, test_file_size);
    create_test_log_file(test_log_file2, test_file_size);

    wal_system = new WAL_IO_System();
    ASSERT_TRUE(wal_system->initialize(64, 8)) << "Failed to initialize WAL I/O system";
  }

  void TearDown() override {
    if (wal_system) {
      wal_system->shutdown();
      delete wal_system;
      wal_system = nullptr;
    }

    std::filesystem::remove_all(test_dir);
  }

  void create_test_log_file(const std::string &file_path, size_t size) {
    int fd = open(file_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    ASSERT_GE(fd, 0) << "Failed to create test log file";

    char buffer[4096];
    memset(buffer, 0xAB, sizeof(buffer));

    for (size_t written = 0; written < size; written += sizeof(buffer)) {
      size_t to_write = std::min(sizeof(buffer), size - written);
      ssize_t result = write(fd, buffer, to_write);
      ASSERT_EQ(result, static_cast<ssize_t>(to_write)) << "Failed to write test data";
    }

    close(fd);
  }

  std::string test_dir;
  std::string test_log_file;
  std::string test_log_file2;
  WAL_IO_System *wal_system = nullptr;
  static constexpr size_t test_file_size = 64 * 1024;  // 64KB
};

TEST_F(WALIOSystemTest, Initialization) {
  WAL_IO_System system;
  EXPECT_TRUE(system.initialize(32, 4));
  EXPECT_FALSE(system.initialize(32, 4));  // Double initialization should fail
  system.shutdown();
}

TEST_F(WALIOSystemTest, OpenCloseWALFile) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  EXPECT_GE(file_index, 0);
  EXPECT_TRUE(wal_system->is_file_open(file_index));
  EXPECT_EQ(wal_system->get_file_size(file_index), test_file_size);

  EXPECT_TRUE(wal_system->close_wal_file(file_index));
  EXPECT_FALSE(wal_system->is_file_open(file_index));
}

TEST_F(WALIOSystemTest, OpenNonExistentFile) {
  int file_index = wal_system->open_wal_file("/tmp/nonexistent_file.log", 1024);
  EXPECT_GE(file_index, 0);  // Should create the file
  EXPECT_TRUE(wal_system->close_wal_file(file_index));

  // Clean up
  unlink("/tmp/nonexistent_file.log");
}

TEST_F(WALIOSystemTest, OpenInvalidPath) {
  int file_index = wal_system->open_wal_file("/root/restricted_file.log", 1024);
  EXPECT_EQ(file_index, -1);  // Should fail due to permissions
}

TEST_F(WALIOSystemTest, SyncReadWrite) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char write_buffer[4096];
  char read_buffer[4096];
  memset(write_buffer, 0xCD, sizeof(write_buffer));
  memset(read_buffer, 0, sizeof(read_buffer));

  // Write data
  ssize_t written = wal_system->sync_write(file_index, write_buffer, sizeof(write_buffer), 0);
  EXPECT_EQ(written, sizeof(write_buffer));

  // Read back
  ssize_t read_bytes = wal_system->sync_read(file_index, read_buffer, sizeof(read_buffer), 0);
  EXPECT_EQ(read_bytes, sizeof(read_buffer));
  EXPECT_EQ(memcmp(write_buffer, read_buffer, sizeof(write_buffer)), 0);

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, SyncReadWriteAtOffset) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char write_buffer[1024];
  char read_buffer[1024];
  memset(write_buffer, 0xEF, sizeof(write_buffer));
  memset(read_buffer, 0, sizeof(read_buffer));

  off_t offset = 8192;

  // Write at offset
  ssize_t written = wal_system->sync_write(file_index, write_buffer, sizeof(write_buffer), offset);
  EXPECT_EQ(written, sizeof(write_buffer));

  // Read back from offset
  ssize_t read_bytes = wal_system->sync_read(file_index, read_buffer, sizeof(read_buffer), offset);
  EXPECT_EQ(read_bytes, sizeof(read_buffer));
  EXPECT_EQ(memcmp(write_buffer, read_buffer, sizeof(write_buffer)), 0);

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, InvalidFileIndex) {
  char buffer[1024];

  // Test with invalid file index
  EXPECT_EQ(wal_system->sync_read(999, buffer, sizeof(buffer), 0), -1);
  EXPECT_EQ(wal_system->sync_write(999, buffer, sizeof(buffer), 0), -1);
  EXPECT_FALSE(wal_system->close_wal_file(999));
  EXPECT_FALSE(wal_system->is_file_open(999));
  EXPECT_EQ(wal_system->get_file_size(999), 0);
}

TEST_F(WALIOSystemTest, AsyncReadWrite) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char write_buffer[2048];
  char read_buffer[2048];
  memset(write_buffer, 0x12, sizeof(write_buffer));
  memset(read_buffer, 0, sizeof(read_buffer));

  // Async write
  int write_op_id = wal_system->async_write(file_index, write_buffer, sizeof(write_buffer), 4096);
  EXPECT_GE(write_op_id, 0);

  // Wait for write completion
  EXPECT_TRUE(wal_system->wait_for_completion(write_op_id, 1000));

  // Async read
  int read_op_id = wal_system->async_read(file_index, read_buffer, sizeof(read_buffer), 4096);
  EXPECT_GE(read_op_id, 0);

  // Wait for read completion
  EXPECT_TRUE(wal_system->wait_for_completion(read_op_id, 1000));

  // Verify data
  EXPECT_EQ(memcmp(write_buffer, read_buffer, sizeof(write_buffer)), 0);

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, ScatterGatherIOContiguous) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  // Test contiguous scatter-gather (should use vectored I/O)
  char buffer1[1024], buffer2[1024], buffer3[1024];
  char read_buffer1[1024], read_buffer2[1024], read_buffer3[1024];

  memset(buffer1, 0x11, sizeof(buffer1));
  memset(buffer2, 0x22, sizeof(buffer2));
  memset(buffer3, 0x33, sizeof(buffer3));

  WAL_iovec_t write_iovec[] = {
    {buffer1, sizeof(buffer1), 0},     // Offset 0
    {buffer2, sizeof(buffer2), 1024},  // Offset 1024 (contiguous)
    {buffer3, sizeof(buffer3), 2048}   // Offset 2048 (contiguous)
  };

  WAL_iovec_t read_iovec[] = {
    {read_buffer1, sizeof(read_buffer1), 0}, {read_buffer2, sizeof(read_buffer2), 1024}, {read_buffer3, sizeof(read_buffer3), 2048}
  };

  // Gather write (should use vectored I/O)
  int write_op_id = wal_system->gather_write(file_index, write_iovec, 3);
  EXPECT_GE(write_op_id, 0);

  // Wait for write completion
  EXPECT_TRUE(wal_system->wait_for_completion(write_op_id, 1000));

  // Scatter read (should use vectored I/O)
  int read_op_id = wal_system->scatter_read(file_index, read_iovec, 3);
  EXPECT_GE(read_op_id, 0);

  // Wait for read completion
  EXPECT_TRUE(wal_system->wait_for_completion(read_op_id, 1000));

  // Verify data
  EXPECT_EQ(memcmp(buffer1, read_buffer1, sizeof(buffer1)), 0);
  EXPECT_EQ(memcmp(buffer2, read_buffer2, sizeof(buffer2)), 0);
  EXPECT_EQ(memcmp(buffer3, read_buffer3, sizeof(buffer3)), 0);

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, ScatterGatherIONonContiguous) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  // Test non-contiguous scatter-gather (should use multiple individual operations)
  char buffer1[512], buffer2[512], buffer3[512];
  char read_buffer1[512], read_buffer2[512], read_buffer3[512];

  memset(buffer1, 0xAA, sizeof(buffer1));
  memset(buffer2, 0xBB, sizeof(buffer2));
  memset(buffer3, 0xCC, sizeof(buffer3));

  WAL_iovec_t write_iovec[] = {
    {buffer1, sizeof(buffer1), 0},     // Offset 0
    {buffer2, sizeof(buffer2), 1024},  // Offset 1024 (gap of 512 bytes)
    {buffer3, sizeof(buffer3), 4096}   // Offset 4096 (non-contiguous)
  };

  WAL_iovec_t read_iovec[] = {
    {read_buffer1, sizeof(read_buffer1), 0}, {read_buffer2, sizeof(read_buffer2), 1024}, {read_buffer3, sizeof(read_buffer3), 4096}
  };

  // Gather write (should use multiple individual operations)
  int write_op_id = wal_system->gather_write(file_index, write_iovec, 3);
  EXPECT_GE(write_op_id, 0);

  // Process completions to handle multiple operations
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  wal_system->process_completions(10, 100);

  // Scatter read (should use multiple individual operations)
  int read_op_id = wal_system->scatter_read(file_index, read_iovec, 3);
  EXPECT_GE(read_op_id, 0);

  // Process completions to handle multiple operations
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  wal_system->process_completions(10, 100);

  // Verify data
  EXPECT_EQ(memcmp(buffer1, read_buffer1, sizeof(buffer1)), 0);
  EXPECT_EQ(memcmp(buffer2, read_buffer2, sizeof(buffer2)), 0);
  EXPECT_EQ(memcmp(buffer3, read_buffer3, sizeof(buffer3)), 0);

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, ScatterGatherIOSingleBuffer) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  // Test single buffer (should fall back to individual operations)
  char buffer[1024];
  char read_buffer[1024];

  memset(buffer, 0xDD, sizeof(buffer));

  WAL_iovec_t write_iovec[] = {{buffer, sizeof(buffer), 0}};

  WAL_iovec_t read_iovec[] = {{read_buffer, sizeof(read_buffer), 0}};

  // Gather write with single buffer
  int write_op_id = wal_system->gather_write(file_index, write_iovec, 1);
  EXPECT_GE(write_op_id, 0);

  // Wait for completion
  EXPECT_TRUE(wal_system->wait_for_completion(write_op_id, 1000));

  // Scatter read with single buffer
  int read_op_id = wal_system->scatter_read(file_index, read_iovec, 1);
  EXPECT_GE(read_op_id, 0);

  // Wait for completion
  EXPECT_TRUE(wal_system->wait_for_completion(read_op_id, 1000));

  // Verify data
  EXPECT_EQ(memcmp(buffer, read_buffer, sizeof(buffer)), 0);

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, FlushFile) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char buffer[1024];
  memset(buffer, 0x77, sizeof(buffer));

  // Write some data
  ssize_t written = wal_system->sync_write(file_index, buffer, sizeof(buffer), 0);
  EXPECT_EQ(written, sizeof(buffer));

  // Flush with metadata sync
  EXPECT_TRUE(wal_system->flush_file(file_index, true));

  // Flush without metadata sync
  EXPECT_TRUE(wal_system->flush_file(file_index, false));

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, AsyncFlush) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char buffer[1024];
  memset(buffer, 0x88, sizeof(buffer));

  // Write some data first
  ssize_t written = wal_system->sync_write(file_index, buffer, sizeof(buffer), 0);
  EXPECT_EQ(written, sizeof(buffer));

  // Async flush with metadata sync
  int flush_op_id = wal_system->async_flush(file_index, true);
  EXPECT_GE(flush_op_id, 0);

  // Wait for flush completion
  EXPECT_TRUE(wal_system->wait_for_completion(flush_op_id, 1000));

  // Async flush without metadata sync (fdatasync)
  int flush_op_id2 = wal_system->async_flush(file_index, false);
  EXPECT_GE(flush_op_id2, 0);

  // Wait for flush completion
  EXPECT_TRUE(wal_system->wait_for_completion(flush_op_id2, 1000));

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, AsyncFlushWithCallback) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char buffer[512];
  memset(buffer, 0x99, sizeof(buffer));

  // Write some data
  ssize_t written = wal_system->sync_write(file_index, buffer, sizeof(buffer), 0);
  EXPECT_EQ(written, sizeof(buffer));

  // Test callback data
  int callback_data = 12345;
  int flush_op_id = wal_system->async_flush(file_index, true, &callback_data);
  EXPECT_GE(flush_op_id, 0);

  // Wait for completion
  EXPECT_TRUE(wal_system->wait_for_completion(flush_op_id, 1000));

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, InvalidFlushOperations) {
  // Test flush on invalid file index
  EXPECT_FALSE(wal_system->flush_file(999, true));
  EXPECT_EQ(wal_system->async_flush(999, true), -1);
}

TEST_F(WALIOSystemTest, WriteFlushSequence) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char buffer[1024];
  memset(buffer, 0xAA, sizeof(buffer));

  // Perform a sequence of async writes followed by async flush
  std::vector<int> write_ops;
  for (int i = 0; i < 3; ++i) {
    int write_op = wal_system->async_write(file_index, buffer, sizeof(buffer), i * sizeof(buffer));
    if (write_op >= 0) {
      write_ops.push_back(write_op);
    }
  }

  // Wait for all writes to complete
  for (int op_id : write_ops) {
    EXPECT_TRUE(wal_system->wait_for_completion(op_id, 1000));
  }

  // Now flush
  int flush_op = wal_system->async_flush(file_index, false);  // fdatasync
  EXPECT_GE(flush_op, 0);
  EXPECT_TRUE(wal_system->wait_for_completion(flush_op, 1000));

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, ConcurrentFlushOperations) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char buffer[256];
  memset(buffer, 0xBB, sizeof(buffer));

  // Write some data
  wal_system->sync_write(file_index, buffer, sizeof(buffer), 0);

  // Submit multiple flush operations
  std::vector<int> flush_ops;
  for (int i = 0; i < 3; ++i) {
    int flush_op = wal_system->async_flush(file_index, i % 2 == 0);  // Alternate fsync/fdatasync
    if (flush_op >= 0) {
      flush_ops.push_back(flush_op);
    }
  }

  // Wait for all flushes to complete
  for (int op_id : flush_ops) {
    EXPECT_TRUE(wal_system->wait_for_completion(op_id, 2000));
  }

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, ProcessCompletions) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char buffer[1024];
  memset(buffer, 0x88, sizeof(buffer));

  // Submit multiple async operations
  std::vector<int> op_ids;
  for (int i = 0; i < 5; ++i) {
    int op_id = wal_system->async_write(file_index, buffer, sizeof(buffer), i * sizeof(buffer));
    if (op_id >= 0) {
      op_ids.push_back(op_id);
    }
  }

  // Process completions
  int processed = wal_system->process_completions(10, 500);
  EXPECT_GE(processed, 0);

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, MultipleFiles) {
  int file_index1 = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  int file_index2 = wal_system->open_wal_file(test_log_file2.c_str(), test_file_size);

  EXPECT_GE(file_index1, 0);
  EXPECT_GE(file_index2, 0);
  EXPECT_NE(file_index1, file_index2);

  EXPECT_TRUE(wal_system->is_file_open(file_index1));
  EXPECT_TRUE(wal_system->is_file_open(file_index2));

  char buffer1[512], buffer2[512];
  memset(buffer1, 0x91, sizeof(buffer1));
  memset(buffer2, 0x92, sizeof(buffer2));

  // Write to both files
  EXPECT_EQ(wal_system->sync_write(file_index1, buffer1, sizeof(buffer1), 0), sizeof(buffer1));
  EXPECT_EQ(wal_system->sync_write(file_index2, buffer2, sizeof(buffer2), 0), sizeof(buffer2));

  // Read back and verify
  char read_buffer1[512], read_buffer2[512];
  EXPECT_EQ(wal_system->sync_read(file_index1, read_buffer1, sizeof(read_buffer1), 0), sizeof(read_buffer1));
  EXPECT_EQ(wal_system->sync_read(file_index2, read_buffer2, sizeof(read_buffer2), 0), sizeof(read_buffer2));

  EXPECT_EQ(memcmp(buffer1, read_buffer1, sizeof(buffer1)), 0);
  EXPECT_EQ(memcmp(buffer2, read_buffer2, sizeof(buffer2)), 0);

  wal_system->close_wal_file(file_index1);
  wal_system->close_wal_file(file_index2);
}

TEST_F(WALIOSystemTest, FileIndexReuse) {
  // Open and close a file multiple times to test index reuse
  std::vector<int> file_indices;

  for (int i = 0; i < 3; ++i) {
    int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
    EXPECT_GE(file_index, 0);
    file_indices.push_back(file_index);
    wal_system->close_wal_file(file_index);
  }

  // Indices should be reused
  EXPECT_EQ(file_indices[0], file_indices[1]);
  EXPECT_EQ(file_indices[1], file_indices[2]);
}

TEST_F(WALIOSystemTest, ConcurrentOperations) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  const int num_threads = 4;
  const int ops_per_thread = 10;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, file_index, t, ops_per_thread]() {
      char buffer[256];
      memset(buffer, 0x50 + t, sizeof(buffer));

      for (int i = 0; i < ops_per_thread; ++i) {
        off_t offset = (t * ops_per_thread + i) * sizeof(buffer);
        if (offset + sizeof(buffer) <= test_file_size) {
          ssize_t result = wal_system->sync_write(file_index, buffer, sizeof(buffer), offset);
          EXPECT_EQ(result, sizeof(buffer));
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, InvalidScatterGatherParams) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  // Test with null iovec
  EXPECT_EQ(wal_system->scatter_read(file_index, nullptr, 1), -1);
  EXPECT_EQ(wal_system->gather_write(file_index, nullptr, 1), -1);

  // Test with zero count
  WAL_iovec_t iovec = {nullptr, 0, 0};
  EXPECT_EQ(wal_system->scatter_read(file_index, &iovec, 0), -1);
  EXPECT_EQ(wal_system->gather_write(file_index, &iovec, 0), -1);

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, ScatterGatherLargeVectorCount) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  // Test with many small buffers that would exceed IOV_MAX for vectored I/O
  constexpr size_t buffer_count = 10;  // Small number for testing
  constexpr size_t buffer_size = 256;

  std::vector<char> write_buffers(buffer_count * buffer_size);
  std::vector<char> read_buffers(buffer_count * buffer_size);
  std::vector<WAL_iovec_t> write_iovecs(buffer_count);
  std::vector<WAL_iovec_t> read_iovecs(buffer_count);

  // Initialize buffers and iovecs for contiguous I/O
  for (size_t i = 0; i < buffer_count; ++i) {
    char pattern = static_cast<char>(0x10 + i);
    memset(&write_buffers[i * buffer_size], pattern, buffer_size);

    write_iovecs[i] = {&write_buffers[i * buffer_size], buffer_size, static_cast<off_t>(i * buffer_size)};
    read_iovecs[i] = {&read_buffers[i * buffer_size], buffer_size, static_cast<off_t>(i * buffer_size)};
  }

  // Gather write (should use vectored I/O if buffer_count <= IOV_MAX)
  int write_op_id = wal_system->gather_write(file_index, write_iovecs.data(), buffer_count);
  EXPECT_GE(write_op_id, 0);

  // Wait for completion
  EXPECT_TRUE(wal_system->wait_for_completion(write_op_id, 2000));

  // Scatter read (should use vectored I/O if buffer_count <= IOV_MAX)
  int read_op_id = wal_system->scatter_read(file_index, read_iovecs.data(), buffer_count);
  EXPECT_GE(read_op_id, 0);

  // Wait for completion
  EXPECT_TRUE(wal_system->wait_for_completion(read_op_id, 2000));

  // Verify data
  for (size_t i = 0; i < buffer_count; ++i) {
    EXPECT_EQ(memcmp(&write_buffers[i * buffer_size], &read_buffers[i * buffer_size], buffer_size), 0)
      << "Buffer " << i << " mismatch";
  }

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, Statistics) {
  int file_index = wal_system->open_wal_file(test_log_file.c_str(), test_file_size);
  ASSERT_GE(file_index, 0);

  char buffer[1024];
  memset(buffer, 0x99, sizeof(buffer));

  uint64_t initial_reads = wal_system->stats.total_reads.load();
  uint64_t initial_writes = wal_system->stats.total_writes.load();

  // Perform some operations
  wal_system->sync_write(file_index, buffer, sizeof(buffer), 0);
  wal_system->sync_read(file_index, buffer, sizeof(buffer), 0);

  // Check statistics
  EXPECT_GT(wal_system->stats.total_reads.load(), initial_reads);
  EXPECT_GT(wal_system->stats.total_writes.load(), initial_writes);
  EXPECT_GT(wal_system->stats.total_bytes_read.load(), 0);
  EXPECT_GT(wal_system->stats.total_bytes_written.load(), 0);

  wal_system->close_wal_file(file_index);
}

TEST_F(WALIOSystemTest, WaitForCompletionTimeout) {
  // Test timeout behavior
  EXPECT_FALSE(wal_system->wait_for_completion(999999, 10));  // Invalid op_id, short timeout
}

// Test global functions
class GlobalWALIOTest : public ::testing::Test {
 protected:
  void TearDown() override { wal_io_system_shutdown(); }
};

TEST_F(GlobalWALIOTest, GlobalInitShutdown) {
  EXPECT_TRUE(wal_io_system_init());
  EXPECT_NE(g_wal_io_system, nullptr);

  // Double init should fail
  EXPECT_FALSE(wal_io_system_init());

  wal_io_system_shutdown();
  EXPECT_EQ(g_wal_io_system, nullptr);

  // Multiple shutdowns should be safe
  wal_io_system_shutdown();
}