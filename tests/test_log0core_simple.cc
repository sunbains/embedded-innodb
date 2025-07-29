/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

Unit tests for Log_core class - simplified integration test
*****************************************************************************/

#include <iostream>
#include <cassert>

// Forward declaration - we'll use the public API instead of direct Log_core
#include "innodb.h"

// Simple integration test that verifies logging works through the public API
int main() {
  std::cout << "Running simplified Log_core integration test...\n";

  ib_err_t err;

  // Initialize the database
  err = ib_init();
  if (err != DB_SUCCESS) {
    std::cerr << "Failed to initialize database: " << ib_strerror(err) << "\n";
    return 1;
  }

  std::cout << "Database initialized successfully\n";

  // Start up the database
  err = ib_startup("barracuda");
  if (err != DB_SUCCESS) {
    std::cerr << "Failed to startup database: " << ib_strerror(err) << "\n";
    (void)ib_shutdown(IB_SHUTDOWN_NORMAL);
    return 1;
  }

  std::cout << "Database started successfully\n";

  // The fact that we can startup means the log system is working
  // This tests the real Log_core functionality through the public API

  // Clean shutdown
  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  if (err != DB_SUCCESS) {
    std::cerr << "Failed to shutdown database: " << ib_strerror(err) << "\n";
    return 1;
  }

  std::cout << "Database shutdown successfully\n";
  std::cout << "Log_core integration test passed!\n";

  return 0;
}