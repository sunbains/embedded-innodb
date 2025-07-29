/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

Logger symbol definitions for tests
*****************************************************************************/

#include "innodb.h"

// Logger symbol definitions for tests
namespace logger {
  int level = static_cast<int>(Debug);  // Default to Debug level for tests
  const char* Progname = "innodb-test";
}