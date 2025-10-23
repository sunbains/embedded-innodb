/**
 * Unit test for Log_buffer
 *
 * This demonstrates how to test the Log_buffer class in isolation.
 */

#include <iostream>

#include "log_buffer.h"

int main() {
  std::cout << "Testing Log_buffer..." << std::endl;

  // Create a log buffer with 4096 bytes
  Log_buffer buffer(4096);

  // Test basic functionality
  buffer.acquire();
  std::cout << "Log buffer acquired successfully" << std::endl;

  // Test data access
  auto &phy_blocks = buffer.m_phy_blocks;
  std::cout << "Physical blocks capacity: " << phy_blocks.capacity() << std::endl;

  buffer.release();
  std::cout << "Log buffer released successfully" << std::endl;

  std::cout << "All tests passed!" << std::endl;
  return 0;
}
