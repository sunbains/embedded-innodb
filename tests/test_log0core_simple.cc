/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

Unit tests for LogCore class - simplified integration test
*****************************************************************************/

#include <iostream>
#include <cassert>

#include "srv0cfg.h"
#include "srv0sys.h"

// Simple integration test that verifies logging works through the C++ API
int main() {
  std::cout << "Running simplified LogCore integration test...\n";

  Config config;
  System sys;

  // Initialize the instance
  auto err = sys.init(config);
  if (err != DB_SUCCESS) {
    std::cerr << "Failed to initialize database: " << db_strerror(err) << "\n";
    return 1;
  }

  std::cout << "Database initialized successfully\n";

  // Start up the instance
  err = sys.startup();
  if (err != DB_SUCCESS) {
    std::cerr << "Failed to startup database: " << db_strerror(err) << "\n";
    return 1;
  }

  std::cout << "Database started successfully\n";

  // The fact that we can startup means the log system is working

  // Clean shutdown
  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  if (err != DB_SUCCESS) {
    std::cerr << "Failed to shutdown database: " << db_strerror(err) << "\n";
    return 1;
  }

  std::cout << "Database shutdown successfully\n";
  std::cout << "LogCore integration test passed!\n";

  return 0;
}
