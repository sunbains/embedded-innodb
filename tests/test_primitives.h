/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

Test configuration using production primitives - refactored from test_config.h
*****************************************************************************/

#pragma once

// Use production primitives
#include "../include/ut0primitives.h"

// Include production log constants instead of duplicating them
#include "../include/log0config.h"

// Mock configuration for testing - abstracted from specific test requirements
struct Test_config {
  ulint m_log_buffer_size = 1024; // 1024 pages = 16MB with 16KB pages

  // Add other configurable test parameters as needed
  bool m_debug_sync_enabled = true;
  ulint m_max_test_duration_ms = 60000; // 60 seconds default
};

// Backwards compatibility alias
using test_srv_config_t = Test_config;

// Global test configuration instance
extern Test_config g_test_config;

// Backwards compatibility global reference
#define test_srv_config g_test_config

// Compatibility macro for existing code that uses srv_config
#define srv_config g_test_config

// Required constants for testing - computed from configuration
#undef LOG_BUFFER_SIZE
#define LOG_BUFFER_SIZE (g_test_config.m_log_buffer_size * UNIV_PAGE_SIZE)

// Test-specific event creation wrapper that can be mocked if needed
inline Cond_var *test_event_create(const char *name) {
  return os_event_create(name);
}

inline void test_event_set(Cond_var *event) { os_event_set(event); }

inline void test_event_free(Cond_var *event) { os_event_free(event); }

// Test memory allocator wrappers - can be instrumented for leak detection
inline void *test_mem_alloc(ulint size) { return mem_alloc(size); }

inline void test_mem_free(void *ptr) { mem_free(ptr); }