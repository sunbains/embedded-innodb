/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

Validation test for refactored primitives
*****************************************************************************/

#include "test_config.h"
#include <cassert>
#include <iostream>

int main() {
  std::cout << "Testing refactored primitives...\n";

  // Test type definitions
  ulint test_ulint = 42;
  lsn_t test_lsn = 12345;
  byte test_byte = 0xFF;
  space_id_t test_space_id = 100;

  // Test constants
  assert(UNIV_PAGE_SIZE == 16384);
  assert(IB_FILE_BLOCK_SIZE == 512);

  // Test debug assertions (these should not abort in release mode)
  ut_ad(true);
  ut_a(true);

  // Test alignment functions
  void *ptr = reinterpret_cast<void *>(0x1234);
  void *aligned_down = ut_align_down(ptr, 16);
  void *aligned_up = ut_align(ptr, 16);

  assert(reinterpret_cast<uintptr_t>(aligned_down) % 16 == 0);
  assert(reinterpret_cast<uintptr_t>(aligned_up) % 16 == 0);
  assert(aligned_down <= ptr);
  assert(aligned_up >= ptr);

  // Test machine data functions
  byte buffer[8];
  uint32_t test_val32 = 0x12345678;
  uint16_t test_val16 = 0x1234;
  uint64_t test_val64 = 0x123456789ABCDEF0ULL;

  mach_write_to_4(buffer, test_val32);
  assert(mach_read_from_4(buffer) == test_val32);

  mach_write_to_2(buffer, test_val16);
  assert(mach_read_from_2(buffer) == test_val16);

  mach_write_to_8(buffer, test_val64);
  assert(mach_read_from_8(buffer) == test_val64);

  // Test memory allocation
  void *mem_ptr = test_mem_alloc(100);
  assert(mem_ptr != nullptr);
  test_mem_free(mem_ptr);

  // Test configuration
  assert(srv_config.m_log_buffer_size == 1024);
  assert(LOG_BUFFER_SIZE == 1024 * UNIV_PAGE_SIZE);

  // Test event system
  Cond_var *event = test_event_create("test_event");
  assert(event != nullptr);
  test_event_set(event);
  test_event_free(event);

  std::cout << "All primitive tests passed!\n";
  return 0;
}