#pragma once

#include "innodb0types.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#define UNIV_MEM_VALID(addr, size) VALGRIND_MAKE_MEM_DEFINED(addr, size)
#define UNIV_MEM_INVALID(addr, size) VALGRIND_MAKE_MEM_UNDEFINED(addr, size)
#define UNIV_MEM_FREE(addr, size) VALGRIND_MAKE_MEM_NOACCESS(addr, size)
#define UNIV_MEM_ALLOC(addr, size) VALGRIND_MAKE_MEM_UNDEFINED(addr, size)
#define UNIV_MEM_DESC(addr, size, b) VALGRIND_CREATE_BLOCK(addr, size, b)
#define UNIV_MEM_UNDESC(b) VALGRIND_DISCARD(b)
#define UNIV_MEM_ASSERT_RW(addr, size)                                         \
  do {                                                                         \
    const void *_p =                                                           \
        (const void *)(ulint)VALGRIND_CHECK_MEM_IS_DEFINED(addr, size);        \
    if (likely_null(_p))                                                       \
      fprintf(stderr, "%s:%d: %p[%u] undefined at %ld\n", __FILE__, __LINE__,  \
              (const void *)(addr), (unsigned)(size),                          \
              (long)(((const char *)_p) - ((const char *)(addr))));            \
  } while (0)
#define UNIV_MEM_ASSERT_W(addr, size)                                          \
  do {                                                                         \
    const void *_p =                                                           \
        (const void *)(ulint)VALGRIND_CHECK_MEM_IS_ADDRESSABLE(addr, size);    \
    if (likely_null(_p))                                                       \
      fprintf(stderr, "%s:%d: %p[%u] unwritable at %ld\n", __FILE__, __LINE__, \
              (const void *)(addr), (unsigned)(size),                          \
              (long)(((const char *)_p) - ((const char *)(addr))));            \
  } while (0)
#else
#define UNIV_MEM_VALID(addr, size)                                             \
  do {                                                                         \
  } while (0)
#define UNIV_MEM_INVALID(addr, size)                                           \
  do {                                                                         \
  } while (0)
#define UNIV_MEM_FREE(addr, size)                                              \
  do {                                                                         \
  } while (0)
#define UNIV_MEM_ALLOC(addr, size)                                             \
  do {                                                                         \
  } while (0)
#define UNIV_MEM_DESC(addr, size, b)                                           \
  do {                                                                         \
  } while (0)
#define UNIV_MEM_UNDESC(b)                                                     \
  do {                                                                         \
  } while (0)
#define UNIV_MEM_ASSERT_RW(addr, size)                                         \
  do {                                                                         \
  } while (0)
#define UNIV_MEM_ASSERT_W(addr, size)                                          \
  do {                                                                         \
  } while (0)
#endif /* UNIV_DEBUG_VALGRIND */

#define UNIV_MEM_ASSERT_AND_FREE(addr, size)                                   \
  do {                                                                         \
    UNIV_MEM_ASSERT_W(addr, size);                                             \
    UNIV_MEM_FREE(addr, size);                                                 \
  } while (0)

#define UNIV_MEM_ASSERT_AND_ALLOC(addr, size)                                  \
  do {                                                                         \
    UNIV_MEM_ASSERT_W(addr, size);                                             \
    UNIV_MEM_ALLOC(addr, size);                                                \
  } while (0)
