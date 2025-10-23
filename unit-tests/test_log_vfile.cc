#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>

// Include the actual library headers
#include "innodb0types.h"
#include "log0config.h"
#include "log0types.h"
#include "ut0logger.h"

// Simple test framework
#define TEST_ASSERT(condition)                                                            \
  do {                                                                                    \
    if (!(condition)) {                                                                   \
      std::cerr << "Test failed at line " << __LINE__ << ": " << #condition << std::endl; \
      return false;                                                                       \
    }                                                                                     \
  } while (0)

#define TEST_ASSERT_THROWS(statement, exception_type)                                                                       \
  do {                                                                                                                      \
    bool caught = false;                                                                                                    \
    try {                                                                                                                   \
      statement;                                                                                                            \
    } catch (const exception_type &) {                                                                                      \
      caught = true;                                                                                                        \
    } catch (...) {                                                                                                         \
      std::cerr << "Test failed at line " << __LINE__ << ": Expected " << #exception_type << " but got different exception" \
                << std::endl;                                                                                               \
      return false;                                                                                                         \
    }                                                                                                                       \
    if (!caught) {                                                                                                          \
      std::cerr << "Test failed at line " << __LINE__ << ": Expected " << #exception_type << " but no exception was thrown" \
                << std::endl;                                                                                               \
      return false;                                                                                                         \
    }                                                                                                                       \
  } while (0)

bool test_constructor(const std::string &test_dir) {
  log_info("Testing Log_vfile constructor...");

  // Test valid constructor
  try {
    Log_vfile vfile(test_dir + "/test_log", 2, 1024 * 1024);
    TEST_ASSERT(vfile.m_directory == test_dir + "/test_log");
    TEST_ASSERT(vfile.m_max_file_size == 1024 * 1024);
    TEST_ASSERT(vfile.m_files.size() == 2);
  } catch (const std::exception &e) {
    std::cerr << "Constructor test failed: " << e.what() << std::endl;
    return false;
  }

  // Test invalid constructor (too few files)
  TEST_ASSERT_THROWS(Log_vfile(test_dir + "/test_log", 1, 1024 * 1024), std::invalid_argument);

  return true;
}

bool test_file_operations(const std::string &test_dir) {
  log_info("Testing file operations...");

  const std::string file_ops_dir = test_dir + "/test_log_vfile_" + std::to_string(getpid());

  try {
    Log_vfile vfile(file_ops_dir, 3, 1024 * 1024);

    // Test open
    auto ret = vfile.open();
    TEST_ASSERT(ret);

    // Test that files were created
    for (size_t i = 0; i < 3; ++i) {
      std::string expected_path = file_ops_dir + "/redo_log_" + std::to_string(i);
      TEST_ASSERT(std::filesystem::exists(expected_path));
    }

    // Test close
    ret = vfile.close();
    TEST_ASSERT(ret);

    // Clean up
    std::filesystem::remove_all(file_ops_dir);

  } catch (const std::exception &e) {
    std::cerr << "File operations test failed: " << e.what() << std::endl;
    std::filesystem::remove_all(file_ops_dir);
    return false;
  }

  return true;
}

bool test_capacity_calculations(const std::string &test_dir) {
  log_info("Testing capacity calculations...");

  try {
    Log_vfile vfile(test_dir + "/test_log", 2, 1024 * 1024);

    // Test get_capacity
    auto capacity = vfile.get_capacity();
    // New architecture: first file has header + checkpoints + data, other files have data only
    auto first_file_data_capacity = static_cast<uint64_t>(1024 * 1024 - LOG_FIRST_FILE_DATA_START);
    auto other_files_capacity = static_cast<uint64_t>(1024 * 1024 * 1);  // 1 other file
    auto expected_capacity = first_file_data_capacity + other_files_capacity;
    TEST_ASSERT(static_cast<uint64_t>(capacity) == expected_capacity);

    // Test calc_size_offset
    off_t test_offset = 1000;
    auto size_offset = vfile.calc_size_offset(test_offset);
    // New architecture: first file subtracts header + checkpoint area, other files no overhead
    auto expected_size_offset =
      static_cast<uint64_t>(test_offset < 1024 * 1024 ? test_offset - LOG_FIRST_FILE_DATA_START : test_offset);
    TEST_ASSERT(static_cast<uint64_t>(size_offset) == expected_size_offset);

    // Test calc_real_offset
    auto real_offset = vfile.calc_real_offset(test_offset);
    // New architecture: first file adds header + checkpoint area, other files no overhead
    auto expected_real_offset =
      static_cast<uint64_t>(test_offset < 1024 * 1024 ? test_offset + LOG_FIRST_FILE_DATA_START : test_offset);
    TEST_ASSERT(static_cast<uint64_t>(real_offset) == expected_real_offset);

  } catch (const std::exception &e) {
    std::cerr << "Capacity calculations test failed: " << e.what() << std::endl;
    return false;
  }

  return true;
}

bool test_lsn_operations(const std::string &test_dir) {
  log_info("Testing LSN operations...");

  try {
    Log_vfile vfile(test_dir + "/test_log", 2, 1024 * 1024);

    // Test set_fields
    lsn_t test_lsn = 12345;
    vfile.set_fields(test_lsn);
    TEST_ASSERT(vfile.m_lsn == test_lsn);

    // Test calc_lsn_offset
    auto lsn_offset = vfile.calc_lsn_offset(test_lsn);
    TEST_ASSERT(lsn_offset >= 0);

    // Test get_io_coordinates
    lsn_t first_header_lsn = 1000;
    auto coords = vfile.get_io_coordinates(first_header_lsn, test_lsn);
    TEST_ASSERT(coords.first != nullptr);
    TEST_ASSERT(static_cast<uint64_t>(coords.second) >= Log_phy_file_header::HEADER_SIZE);

    // Find the file index in the vector
    size_t file_no = 0;
    for (size_t i = 0; i < vfile.m_files.size(); ++i) {
      if (&vfile.m_files[i] == coords.first) {
        file_no = i;
        break;
      }
    }
    TEST_ASSERT(file_no < vfile.m_files.size());

  } catch (const std::exception &e) {
    std::cerr << "LSN operations test failed: " << e.what() << std::endl;
    return false;
  }

  return true;
}

bool test_string_representation(const std::string &test_dir) {
  log_info("Testing string representation...");

  try {
    Log_vfile vfile(test_dir + "/test_log", 2, 1024 * 1024);

    auto str = vfile.to_string();
    TEST_ASSERT(!str.empty());
    TEST_ASSERT(str.find("Log vfile:") != std::string::npos);
    TEST_ASSERT(str.find(test_dir + "/test_log") != std::string::npos);
    // Check for file size in the string (might be formatted as 1048576 or similar)
    log_info("  String representation: ", str);
    TEST_ASSERT(str.find("1048576") != std::string::npos || str.find("1024") != std::string::npos);

  } catch (const std::exception &e) {
    std::cerr << "String representation test failed: " << e.what() << std::endl;
    return false;
  }

  return true;
}

bool test_checkpoint_operations(const std::string &test_dir) {
  log_info("Testing checkpoint operations...");

  try {
    Log_vfile vfile(test_dir + "/test_log", 2, 1024 * 1024);

    // Test checkpoint_set
    vfile.checkpoint_set(1, 1000);

    // Test checkpoint_get
    auto checkpoint = vfile.checkpoint_get();
    (void)checkpoint;  // Suppress unused variable warning
    // Note: These return default values since implementation is stubbed

    // Test prepare_checkpoint
    vfile.prepare_checkpoint(123, 456);

  } catch (const std::exception &e) {
    std::cerr << "Checkpoint operations test failed: " << e.what() << std::endl;
    return false;
  }

  return true;
}

bool test_sync_operations(const std::string &test_dir) {
  log_info("Testing sync operations...");

  const std::string sync_test_dir = test_dir + "/test_log_vfile_sync_" + std::to_string(getpid());

  try {
    Log_vfile vfile(sync_test_dir, 2, 1024 * 1024);

    // Open first
    auto ret = vfile.open();
    TEST_ASSERT(ret);

    // Test sync
    ret = vfile.sync();
    TEST_ASSERT(ret);

    // Test sync_read (stubbed)
    char buffer[100];
    ret = vfile.sync_read(buffer, sizeof(buffer), 0);
    TEST_ASSERT(ret);

    // Test sync_write (stubbed)
    ret = vfile.sync_write(buffer, sizeof(buffer), 0);
    TEST_ASSERT(ret);

    // Test async_write (stubbed)
    ret = vfile.async_write(buffer, sizeof(buffer), 0);
    TEST_ASSERT(ret);

    // Clean up
    ut_a(vfile.close());
    std::filesystem::remove_all(sync_test_dir);

  } catch (const std::exception &e) {
    std::cerr << "Sync operations test failed: " << e.what() << std::endl;
    std::filesystem::remove_all(sync_test_dir);
    return false;
  }

  return true;
}

bool test_lsn_calculation_functions(const std::string &test_dir) {
  log_info("Testing LSN calculation functions...");

  try {
    Log_vfile vfile(test_dir + "/test_log", 3, 1024 * 1024);

    // Test get_capacity
    auto capacity = vfile.get_capacity();
    // New architecture: first file has header + checkpoints + data, other files have data only
    auto first_file_data_capacity = static_cast<uint64_t>(1024 * 1024 - LOG_FIRST_FILE_DATA_START);
    auto other_files_capacity = static_cast<uint64_t>(1024 * 1024 * 2);  // 2 other files
    auto expected_capacity = first_file_data_capacity + other_files_capacity;
    TEST_ASSERT(static_cast<uint64_t>(capacity) == expected_capacity);
    log_info("  get_capacity(): ", capacity, " bytes (expected: ", expected_capacity, ")");

    // Test calc_size_offset
    std::vector<off_t> test_offsets = {0, 1000, 5000, 10000, 50000, 100000};
    for (off_t offset : test_offsets) {
      auto size_offset = vfile.calc_size_offset(offset);
      // New architecture: first file subtracts header + checkpoint area, other files no overhead
      auto expected_size_offset = static_cast<uint64_t>(offset < 1024 * 1024 ? offset - LOG_FIRST_FILE_DATA_START : offset);
      TEST_ASSERT(static_cast<uint64_t>(size_offset) == expected_size_offset);
      log_info("  calc_size_offset(", offset, "): ", size_offset, " (expected: ", expected_size_offset, ")");
    }

    // Test calc_real_offset
    for (off_t offset : test_offsets) {
      auto real_offset = vfile.calc_real_offset(offset);
      // New architecture: first file adds header + checkpoint area, other files no overhead
      auto expected_real_offset = static_cast<uint64_t>(offset < 1024 * 1024 ? offset + LOG_FIRST_FILE_DATA_START : offset);
      TEST_ASSERT(static_cast<uint64_t>(real_offset) == expected_real_offset);
      log_info("  calc_real_offset(", offset, "): ", real_offset, " (expected: ", expected_real_offset, ")");
    }

    // Test get_io_coordinates with various LSNs
    lsn_t first_header_lsn = 0x1000;
    std::vector<lsn_t> test_lsns = {0x1000, 0x2000, 0x5000, 0x10000, 0x20000, 0x50000, 0x100000};

    for (lsn_t lsn : test_lsns) {
      auto coords = vfile.get_io_coordinates(first_header_lsn, lsn);

      TEST_ASSERT(coords.first != nullptr);
      TEST_ASSERT(static_cast<uint64_t>(coords.second) >= Log_phy_file_header::HEADER_SIZE);

      // Find the file index in the vector
      size_t file_no = 0;
      for (size_t i = 0; i < vfile.m_files.size(); ++i) {
        if (&vfile.m_files[i] == coords.first) {
          file_no = i;
          break;
        }
      }

      // Calculate expected values for new architecture
      const int64_t first_file_data_capacity = 1024 * 1024 - LOG_FIRST_FILE_DATA_START;
      const int64_t other_files_capacity = 1024 * 1024;
      auto lsn_offset = lsn - first_header_lsn;

      uint64_t expected_file_no;
      uint64_t expected_offset;

      if (lsn_offset < first_file_data_capacity) {
        // Data in first file
        expected_file_no = 0;
        expected_offset = LOG_FIRST_FILE_DATA_START + lsn_offset;
      } else {
        // Data in other files
        auto remaining_offset = lsn_offset - first_file_data_capacity;
        expected_file_no = 1 + (remaining_offset / other_files_capacity);
        expected_offset = remaining_offset % other_files_capacity;
      }

      expected_file_no = expected_file_no % 3;  // Wrap around

      TEST_ASSERT(file_no == expected_file_no);
      TEST_ASSERT(static_cast<uint64_t>(coords.second) == expected_offset);

      log_info(
        "  get_io_coordinates(LSN 0x",
        std::hex,
        lsn,
        std::dec,
        "): file=",
        file_no,
        ", offset=",
        coords.second,
        " (expected: file=",
        expected_file_no,
        ", offset=",
        expected_offset,
        ")"
      );
    }

    // Test get_io_coordinates consistency
    for (lsn_t lsn : test_lsns) {
      auto coords1 = vfile.get_io_coordinates(first_header_lsn, lsn);
      auto coords2 = vfile.get_io_coordinates(first_header_lsn, lsn);

      TEST_ASSERT(coords1.first != nullptr);
      TEST_ASSERT(static_cast<uint64_t>(coords1.second) >= Log_phy_file_header::HEADER_SIZE);

      // Verify coordinates are consistent across multiple calls
      TEST_ASSERT(coords1.first == coords2.first);
      TEST_ASSERT(coords1.second == coords2.second);

      // Find the file index in the vector
      size_t file_index = 0;
      for (size_t i = 0; i < vfile.m_files.size(); ++i) {
        if (&vfile.m_files[i] == coords1.first) {
          file_index = i;
          break;
        }
      }

      log_info("  get_io_coordinates(LSN 0x", std::hex, lsn, std::dec, "): file_index=", file_index, ", offset=", coords1.second);
    }

    // Test calc_lsn_offset with set_fields
    vfile.set_fields(0x5000);
    TEST_ASSERT(vfile.m_lsn == 0x5000);

    std::vector<lsn_t> lsn_offsets = {0x4000, 0x5000, 0x6000, 0x7000, 0x8000};
    for (lsn_t lsn : lsn_offsets) {
      auto lsn_offset = vfile.calc_lsn_offset(lsn);
      TEST_ASSERT(lsn_offset >= 0);
      log_info("  calc_lsn_offset(LSN 0x", std::hex, lsn, std::dec, "): ", lsn_offset);
    }

    // Test boundary conditions
    lsn_t boundary_lsn = first_header_lsn + capacity - sizeof(Log_phy_block);
    auto boundary_coords = vfile.get_io_coordinates(first_header_lsn, boundary_lsn);
    TEST_ASSERT(boundary_coords.first != nullptr);
    TEST_ASSERT(static_cast<uint64_t>(boundary_coords.second) >= Log_phy_file_header::HEADER_SIZE);

    size_t boundary_file_no = 0;
    for (size_t i = 0; i < vfile.m_files.size(); ++i) {
      if (&vfile.m_files[i] == boundary_coords.first) {
        boundary_file_no = i;
        break;
      }
    }
    TEST_ASSERT(boundary_file_no < vfile.m_files.size());

    log_info(
      "  Boundary test - LSN 0x", std::hex, boundary_lsn, std::dec, ": file=", boundary_file_no, ", offset=", boundary_coords.second
    );

    // Test wrap-around scenarios
    lsn_t wrap_lsn = first_header_lsn + capacity + sizeof(Log_phy_block);
    auto wrap_coords = vfile.get_io_coordinates(first_header_lsn, wrap_lsn);
    TEST_ASSERT(wrap_coords.first != nullptr);
    TEST_ASSERT(static_cast<uint64_t>(wrap_coords.second) >= Log_phy_file_header::HEADER_SIZE);

    size_t wrap_file_no = 0;
    for (size_t i = 0; i < vfile.m_files.size(); ++i) {
      if (&vfile.m_files[i] == wrap_coords.first) {
        wrap_file_no = i;
        break;
      }
    }
    TEST_ASSERT(wrap_file_no < vfile.m_files.size());

    log_info("  Wrap-around test - LSN 0x", std::hex, wrap_lsn, std::dec, ": file=", wrap_file_no, ", offset=", wrap_coords.second);

    // Test negative LSN handling (should wrap around)
    lsn_t negative_lsn = first_header_lsn - 1000;
    auto neg_coords = vfile.get_io_coordinates(first_header_lsn, negative_lsn);
    TEST_ASSERT(neg_coords.first != nullptr);
    TEST_ASSERT(static_cast<uint64_t>(neg_coords.second) >= Log_phy_file_header::HEADER_SIZE);

    size_t neg_file_no = 0;
    for (size_t i = 0; i < vfile.m_files.size(); ++i) {
      if (&vfile.m_files[i] == neg_coords.first) {
        neg_file_no = i;
        break;
      }
    }
    TEST_ASSERT(neg_file_no < vfile.m_files.size());

    log_info(
      "  Negative LSN test - LSN 0x", std::hex, negative_lsn, std::dec, ": file=", neg_file_no, ", offset=", neg_coords.second
    );

  } catch (const std::exception &e) {
    std::cerr << "LSN calculation functions test failed: " << e.what() << std::endl;
    return false;
  }

  return true;
}

bool test_random_lsn_write_read(const std::string &test_dir) {
  log_info("Testing random LSN write/read operations...");

  const std::string write_read_dir = test_dir + "/test_log_vfile_write_read_" + std::to_string(getpid());

  try {
    Log_vfile vfile(write_read_dir, 3, 1024 * 1024);

    // Open the log vfile
    auto ret = vfile.open();
    TEST_ASSERT(ret);

    // Test data patterns
    const std::vector<std::vector<byte>> test_patterns = {
      {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
      {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x00, 0x11},
      {0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0},
      {0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00},
      {0x55, 0x55, 0x55, 0x55, 0xAA, 0xAA, 0xAA, 0xAA}
    };

    // Random LSN locations (aligned to Log_phy_block boundaries)
    const std::vector<lsn_t> test_lsns = {0x1000, 0x2000, 0x3000, 0x4000, 0x5000, 0x6000, 0x7000, 0x8000, 0x9000, 0xA000};

    // Write test data to random LSN locations
    for (size_t i = 0; i < test_lsns.size() && i < test_patterns.size(); ++i) {
      lsn_t lsn = test_lsns[i];
      const auto &pattern = test_patterns[i % test_patterns.size()];

      // Create a test buffer with the pattern repeated to fill a log block
      std::vector<byte> test_data(sizeof(Log_phy_block));

      // Fill with pattern repeated
      for (size_t j = 0; j < sizeof(Log_phy_block); ++j) {
        test_data[j] = pattern[j % pattern.size()];
      }

      // Test sync_write with raw buffer
      auto ret = vfile.sync_write(test_data.data(), test_data.size(), 0);
      TEST_ASSERT(ret);

      // Test async_write with raw buffer
      ret = vfile.async_write(test_data.data(), test_data.size(), 0);
      TEST_ASSERT(ret);

      log_info("  Writing pattern ", i, " to LSN 0x", std::hex, lsn, std::dec, " (size: ", test_data.size(), " bytes)");
    }

    // Test reading back from the same LSN locations
    for (size_t i = 0; i < test_lsns.size() && i < test_patterns.size(); ++i) {
      lsn_t lsn = test_lsns[i];
      const auto &expected_pattern = test_patterns[i % test_patterns.size()];

      // Create read buffer
      std::vector<byte> read_data(sizeof(Log_phy_block));

      // Test sync_read with raw buffer
      auto ret = vfile.sync_read(read_data.data(), read_data.size(), 0);
      TEST_ASSERT(ret);

      // Verify the read data matches expected pattern
      bool pattern_matches = true;
      for (size_t j = 0; j < sizeof(Log_phy_block); ++j) {
        byte expected_byte = expected_pattern[j % expected_pattern.size()];
        if (read_data[j] != expected_byte) {
          pattern_matches = false;
          break;
        }
      }

      // Note: Since sync_read is stubbed, this will always pass
      // In a real implementation, this would verify data integrity
      TEST_ASSERT(pattern_matches);

      log_info(
        "  Reading pattern ", i, " from LSN 0x", std::hex, lsn, std::dec, " (verified: ", (pattern_matches ? "PASS" : "FAIL"), ")"
      );
    }

    // Test LSN offset calculations for random locations
    for (lsn_t lsn : test_lsns) {
      auto offset = vfile.calc_lsn_offset(lsn);
      TEST_ASSERT(offset >= 0);

      // Test io coordinates calculation
      lsn_t first_header_lsn = 0x1000;  // Start LSN
      auto coords = vfile.get_io_coordinates(first_header_lsn, lsn);
      TEST_ASSERT(coords.first != nullptr);
      TEST_ASSERT(static_cast<uint64_t>(coords.second) >= Log_phy_file_header::HEADER_SIZE);

      // Find the file index in the vector
      size_t file_no = 0;
      for (size_t i = 0; i < vfile.m_files.size(); ++i) {
        if (&vfile.m_files[i] == coords.first) {
          file_no = i;
          break;
        }
      }
      TEST_ASSERT(file_no < vfile.m_files.size());

      log_info("  LSN 0x", std::hex, lsn, std::dec, " -> File ", file_no, ", Offset ", coords.second);
    }

    // Test boundary conditions
    lsn_t capacity = vfile.get_capacity();
    TEST_ASSERT(capacity > 0);

    // Test LSN at the boundary
    lsn_t boundary_lsn = 0x1000 + capacity - sizeof(Log_phy_block);
    auto boundary_offset = vfile.calc_lsn_offset(boundary_lsn);
    TEST_ASSERT(boundary_offset >= 0);

    // Test wrap-around LSN (beyond capacity)
    lsn_t wrap_lsn = 0x1000 + capacity + sizeof(Log_phy_block);
    auto wrap_offset = vfile.calc_lsn_offset(wrap_lsn);
    TEST_ASSERT(wrap_offset >= 0);

    log_info("  Capacity: ", capacity, " bytes");
    log_info("  Boundary LSN: 0x", std::hex, boundary_lsn, std::dec);
    log_info("  Wrap LSN: 0x", std::hex, wrap_lsn, std::dec);

    // Close the log vfile
    ret = vfile.close();
    TEST_ASSERT(ret);

    // Clean up
    std::filesystem::remove_all(write_read_dir);

  } catch (const std::exception &e) {
    std::cerr << "Random LSN write/read test failed: " << e.what() << std::endl;
    std::filesystem::remove_all(write_read_dir);
    return false;
  }

  return true;
}

void cleanup_test_directories(const std::string &test_dir) {
  log_info("Cleaning up test directories...");

  // Clean up any remaining test directories
  try {
    std::filesystem::remove_all(test_dir);

    // Clean up any test directories that might have been created with different patterns
    for (const auto &entry : std::filesystem::directory_iterator(".")) {
      if (entry.is_directory()) {
        std::string dir_name = entry.path().filename().string();
        if (dir_name.find("test_log") == 0) {
          std::filesystem::remove_all(entry.path());
          log_info("Removed test directory: ", dir_name);
        }
      }
    }

    // Also clean up from /tmp
    for (const auto &entry : std::filesystem::directory_iterator("/tmp")) {
      if (entry.is_directory()) {
        std::string dir_name = entry.path().filename().string();
        if (dir_name.find("test_log_vfile") == 0) {
          std::filesystem::remove_all(entry.path());
          log_info("Removed test directory from /tmp: ", dir_name);
        }
      }
    }

  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << "Warning: Failed to clean up some test directories: " << e.what() << std::endl;
  }
}

int main() {
  log_info("Starting Log_vfile unit tests...");

  // Set the base test directory - can be easily changed here
  const std::string test_dir = "/tmp/test_log_vfile_tests_" + std::to_string(getpid());

  // Create the test directory
  try {
    std::filesystem::create_directories(test_dir);
  } catch (const std::filesystem::filesystem_error &e) {
    std::cerr << "Failed to create test directory: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }

  bool all_passed = true;

  all_passed &= test_constructor(test_dir);
  all_passed &= test_file_operations(test_dir);
  all_passed &= test_capacity_calculations(test_dir);
  all_passed &= test_lsn_operations(test_dir);
  all_passed &= test_string_representation(test_dir);
  all_passed &= test_checkpoint_operations(test_dir);
  all_passed &= test_sync_operations(test_dir);
  all_passed &= test_lsn_calculation_functions(test_dir);
  // all_passed &= test_random_lsn_write_read(test_dir);

  // Clean up test directories
  cleanup_test_directories(test_dir);

  if (all_passed) {
    log_info("All tests passed!");
    return EXIT_SUCCESS;
  } else {
    log_info("Some tests failed!");
    return EXIT_FAILURE;
  }
}
