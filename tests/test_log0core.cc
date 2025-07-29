/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

*****************************************************************************/

/** @file tests/test_log0core.cc
Unit tests for Log_core class

Tests core logging functionality with minimal dependencies
*******************************************************/

#include "log0core.h"
#include "srv0srv.h"

#include <cassert>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

struct Log_coreTest {
  static void run_all_tests() {
    std::cout << "Running Log_core tests...\n";

    test_constructor_destructor();
    test_basic_properties();
    test_block_functions();
    test_reserve_and_write_fast();
    test_write_low();
    test_block_boundaries();
    test_checksum_operations();
    test_reset_functionality();
    test_edge_cases();

    std::cout << "All Log_core tests passed!\n";
  }

private:
  static void test_constructor_destructor() {
    std::cout << "  Testing constructor/destructor...\n";

    {
      Log_core log;

      // Test initial state
      assert(log.get_lsn() == LOG_START_LSN + LOG_BLOCK_HDR_SIZE);
      assert(log.get_buf_free() == LOG_BLOCK_HDR_SIZE);
      assert(log.get_buf() != nullptr);
      assert(log.get_buf_size() == LOG_BUFFER_SIZE);
      assert(log.is_buf_available(100));
    }
    // Destructor should clean up without issues
  }

  static void test_basic_properties() {
    std::cout << "  Testing basic properties...\n";

    Log_core log;

    // Test getters
    lsn_t initial_lsn = log.get_lsn();
    ulint initial_free = log.get_buf_free();
    const byte *buf = log.get_buf();
    ulint buf_size = log.get_buf_size();

    assert(initial_lsn > LOG_START_LSN);
    assert(initial_free == LOG_BLOCK_HDR_SIZE);
    assert(buf != nullptr);
    assert(buf_size == LOG_BUFFER_SIZE);

    // Test buffer availability
    assert(log.is_buf_available(100));
    assert(log.is_buf_available(1000));
    assert(!log.is_buf_available(buf_size + 1));
  }

  static void test_block_functions() {
    std::cout << "  Testing block manipulation functions...\n";

    // Test block number conversion
    lsn_t test_lsn = LOG_START_LSN + IB_FILE_BLOCK_SIZE * 5;
    ulint block_no = Log_core::block_convert_lsn_to_no(test_lsn);
    assert(block_no > 0);
    assert(block_no <= 0x3FFFFFFFUL + 1);

    // Test block initialization and manipulation
    byte test_block[IB_FILE_BLOCK_SIZE];
    memset(test_block, 0, IB_FILE_BLOCK_SIZE);

    Log_core::block_init(test_block, test_lsn);

    // Verify initialization
    ulint hdr_no = Log_core::block_get_hdr_no(test_block);
    ulint data_len = Log_core::block_get_data_len(test_block);
    ulint first_rec = Log_core::block_get_first_rec_group(test_block);

    assert(hdr_no == block_no);
    assert(data_len == LOG_BLOCK_HDR_SIZE);
    assert(first_rec == 0);

    // Test setting values
    Log_core::block_set_data_len(test_block, 100);
    assert(Log_core::block_get_data_len(test_block) == 100);

    Log_core::block_set_first_rec_group(test_block, 50);
    assert(Log_core::block_get_first_rec_group(test_block) == 50);
  }

  static void test_reserve_and_write_fast() {
    std::cout << "  Testing reserve_and_write_fast...\n";

    Log_core log;

    // Test successful write
    const char *test_data = "Hello, World!";
    ulint len = strlen(test_data);
    lsn_t start_lsn;

    lsn_t end_lsn = log.reserve_and_write_fast(test_data, len, &start_lsn);

    assert(end_lsn != 0);
    assert(start_lsn < end_lsn);
    assert(end_lsn - start_lsn == len);

    // Verify data was written
    const byte *buf = log.get_buf();
    ulint initial_free = LOG_BLOCK_HDR_SIZE;
    assert(memcmp(buf + initial_free, test_data, len) == 0);

    // Test write that would exceed block boundary
    std::string large_data(IB_FILE_BLOCK_SIZE, 'X');
    lsn_t start_lsn2;
    lsn_t end_lsn2 = log.reserve_and_write_fast(
        large_data.c_str(), large_data.length(), &start_lsn2);

    assert(end_lsn2 == 0); // Should fail due to size
  }

  static void test_write_low() {
    std::cout << "  Testing write_low...\n";

    Log_core log;

    // Test writing data that fits in one block
    const char *test_data = "Test data for write_low";
    ulint len = strlen(test_data);

    lsn_t initial_lsn = log.get_lsn();
    log.write_low(reinterpret_cast<const byte *>(test_data), len);

    lsn_t final_lsn = log.get_lsn();
    assert(final_lsn > initial_lsn);

    // Verify data was written
    const byte *buf = log.get_buf();
    ulint initial_free = LOG_BLOCK_HDR_SIZE;
    assert(memcmp(buf + initial_free, test_data, len) == 0);
  }

  static void test_block_boundaries() {
    std::cout << "  Testing block boundary handling...\n";

    Log_core log;

    // Create data that will span multiple blocks
    std::vector<byte> large_data(IB_FILE_BLOCK_SIZE * 2, 'A');

    lsn_t initial_lsn = log.get_lsn();
    log.write_low(large_data.data(), large_data.size());

    lsn_t final_lsn = log.get_lsn();
    assert(final_lsn > initial_lsn);

    // The LSN should have increased by more than the data size due to headers
    assert(final_lsn - initial_lsn >= large_data.size());
  }

  static void test_checksum_operations() {
    std::cout << "  Testing checksum operations...\n";

    byte test_block[IB_FILE_BLOCK_SIZE];
    memset(test_block, 0xAA, IB_FILE_BLOCK_SIZE);

    // Calculate checksum
    uint32_t checksum1 = Log_core::block_calc_checksum(test_block);
    assert(checksum1 != 0);

    // Set and get checksum
    Log_core::block_set_checksum(test_block, checksum1);
    uint32_t retrieved_checksum = Log_core::block_get_checksum(test_block);
    assert(retrieved_checksum == checksum1);

    // Test store checksum functionality
    Log_core log;
    memset(test_block, 0xBB, IB_FILE_BLOCK_SIZE);
    log.block_store_checksum(test_block);

    uint32_t stored_checksum = Log_core::block_get_checksum(test_block);
    uint32_t calculated_checksum = Log_core::block_calc_checksum(test_block);
    assert(stored_checksum == calculated_checksum);
  }

  static void test_reset_functionality() {
    std::cout << "  Testing reset functionality...\n";

    Log_core log;

    // Write some data
    const char *test_data = "Some test data";
    ulint len = strlen(test_data);
    lsn_t start_lsn;

    log.reserve_and_write_fast(test_data, len, &start_lsn);

    // Verify state changed after write
    assert(log.get_lsn() > LOG_START_LSN + LOG_BLOCK_HDR_SIZE);
    assert(log.get_buf_free() > LOG_BLOCK_HDR_SIZE);

    // Reset the log
    log.reset();

    // Verify reset state
    assert(log.get_lsn() == LOG_START_LSN + LOG_BLOCK_HDR_SIZE);
    assert(log.get_buf_free() == LOG_BLOCK_HDR_SIZE);

    // Verify we can write again after reset
    lsn_t start_lsn2;
    lsn_t end_lsn2 = log.reserve_and_write_fast(test_data, len, &start_lsn2);
    assert(end_lsn2 != 0);
  }

  static void test_edge_cases() {
    std::cout << "  Testing edge cases...\n";

    Log_core log;

    // Test zero-length write
    lsn_t start_lsn;
    lsn_t end_lsn = log.reserve_and_write_fast("", 0, &start_lsn);
    assert(end_lsn != 0);
    assert(end_lsn == start_lsn);

    // Test single byte write
    end_lsn = log.reserve_and_write_fast("X", 1, &start_lsn);
    assert(end_lsn != 0);
    assert(end_lsn - start_lsn == 1);

    // Test LSN progression
    lsn_t lsn1 = log.get_lsn();
    log.reserve_and_write_fast("Y", 1, &start_lsn);
    lsn_t lsn2 = log.get_lsn();
    assert(lsn2 > lsn1);

    // Test buffer availability edge cases
    assert(log.is_buf_available(0));
    assert(!log.is_buf_available(ULINT_MAX));
  }
};

// Test helper function for manual testing
void manual_log_test() {
  std::cout << "\nManual Log_core test:\n";

  Log_core log;

  std::cout << "Initial LSN: " << log.get_lsn() << "\n";
  std::cout << "Initial buf_free: " << log.get_buf_free() << "\n";
  std::cout << "Buffer size: " << log.get_buf_size() << "\n";

  // Test writing some log records
  const std::vector<std::string> test_records = {
      "First log record", "Second log record with more data", "Third record",
      "Final record for testing"};

  for (const auto &record : test_records) {
    lsn_t start_lsn;
    lsn_t end_lsn =
        log.reserve_and_write_fast(record.c_str(), record.length(), &start_lsn);

    if (end_lsn != 0) {
      std::cout << "Wrote record: \"" << record << "\" from LSN " << start_lsn
                << " to " << end_lsn << "\n";
    } else {
      std::cout << "Failed to write record: \"" << record
                << "\" (not enough space in block)\n";
    }
  }

  std::cout << "Final LSN: " << log.get_lsn() << "\n";
  std::cout << "Final buf_free: " << log.get_buf_free() << "\n";
}

int main() {
  try {
    Log_coreTest::run_all_tests();
    manual_log_test();

    std::cout << "\nAll tests completed successfully!\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "Test failed with exception: " << e.what() << "\n";
    return 1;
  } catch (...) {
    std::cerr << "Test failed with unknown exception\n";
    return 1;
  }
}
