/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

Logger symbol definitions for unit tests
*****************************************************************************/

#include "innodb.h"

// Logger symbol definitions for unit tests
namespace logger {
  int level = static_cast<int>(Debug);  // Default to Debug level for unit tests  
  const char* Progname = "innodb-unit-test";
}