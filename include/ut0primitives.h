/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

Production primitives interface for embedded InnoDB - For unit tests.
*****************************************************************************/

#pragma once

// Basic type definitions
#include "innodb0types.h"

// Debug utilities
#include "ut0dbg.h"

// Machine data operations
#include "mach0data.h"

// Memory management
#include "mem0mem.h"

// Byte operations and alignment
#include "ut0byte.h"

// OS synchronization primitives
#include "os0sync.h"

// Re-export key primitives with consistent naming for easier transition
// These maintain compatibility with test code while using production implementations

// Type aliases (already defined in innodb0types.h, re-exported for clarity)
using ulint = uintptr_t;
using lsn_t = uint64_t;
using byte = unsigned char;
using space_id_t = uint32_t;

// Debug assertions (from ut0dbg.h)
// ut_ad and ut_a are already available

// Memory alignment functions (from ut0byte.h)
// ut_align_down and ut_align are already available

// Machine data functions (from mach0data.h)
// mach_write_to_* and mach_read_from_* are already available

// Memory management (from mem0mem.h)
// mem_alloc and mem_free are already available

// Event system (from os0sync.h)
// os_event_* functions are already available

// Constants that might be needed (from innodb0types.h and ib0config.h)
// UNIV_PAGE_SIZE, IB_FILE_BLOCK_SIZE, ULINT_MAX are already available

// Source location for debugging (from innodb0types.h)
// Current_location() is already available
