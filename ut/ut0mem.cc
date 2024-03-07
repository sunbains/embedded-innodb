/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc., 59 Temple
Place, Suite 330, Boston, MA 02111-1307 USA

*****************************************************************************/

/** @file ut/ut0mem.c
Memory primitives

Created 5/11/1994 Heikki Tuuri
*************************************************************************/

#include "ut0mem.h"

#include <errno.h>

#ifdef UNIV_NONINL
#include "ut0mem.ic"
#endif

#include "os0thread.h"
#include "srv0srv.h"
#include "srv0start.h"

#include <stdlib.h>

/** This struct is placed first in every allocated memory block */
typedef struct ut_mem_block_struct ut_mem_block_t;

/** The total amount of memory currently allocated from the operating
system with os_mem_alloc_large() or malloc().  Does not count malloc()
if srv_use_sys_malloc is set.  Protected by ut_list_mutex. */
ulint ut_total_allocated_memory = 0;

/** Mutex protecting ut_total_allocated_memory and ut_mem_block_list */
os_fast_mutex_t ut_list_mutex;

/** Dynamically allocated memory block */
struct ut_mem_block_struct {
  UT_LIST_NODE_T(ut_mem_block_t) mem_block_list;
  /*!< mem block list node */
  ulint size;    /*!< size of allocated memory */
  ulint magic_n; /*!< magic number (UT_MEM_MAGIC_N) */
};

/** The value of ut_mem_block_struct::magic_n.  Used in detecting
memory corruption. */
#define UT_MEM_MAGIC_N 1601650166

/** List of all memory blocks allocated from the operating system
with malloc.  Protected by ut_list_mutex. */
static UT_LIST_BASE_NODE_T(ut_mem_block_t) ut_mem_block_list;

/** Flag: has ut_mem_block_list been initialized? */
static bool ut_mem_block_list_inited = false;

/** A dummy pointer for generating a null pointer exception in
ut_malloc_low() */
static ulint *ut_mem_null_ptr = nullptr;

void ut_mem_var_init() {
  ut_total_allocated_memory = 0;

  memset(&ut_mem_block_list, 0x0, sizeof(ut_mem_block_list));

  memset(&ut_list_mutex, 0x0, sizeof(ut_list_mutex));

  ut_mem_block_list_inited = false;

  ut_mem_null_ptr = nullptr;
}

void ut_mem_init() {
  ut_a(!srv_was_started);

  if (!ut_mem_block_list_inited) {
    os_fast_mutex_init(&ut_list_mutex);
    UT_LIST_INIT(ut_mem_block_list);
    ut_mem_block_list_inited = true;
  }
}

void *ut_malloc_low(ulint n, bool set_to_zero, bool assert_on_error) {
  ulint retry_count;
  void *ret;

  if (likely(srv_use_sys_malloc)) {
    ret = malloc(n);
    ut_a(ret || !assert_on_error);

#ifdef UNIV_SET_MEM_TO_ZERO
    if (set_to_zero) {
      memset(ret, '\0', n);
      UNIV_MEM_ALLOC(ret, n);
    }
#endif
    return (ret);
  }

  ut_ad((sizeof(ut_mem_block_t) % 8) == 0); /* check alignment ok */
  ut_a(ut_mem_block_list_inited);

  retry_count = 0;

retry:
  os_fast_mutex_lock(&ut_list_mutex);

  ret = malloc(n + sizeof(ut_mem_block_t));

  if (ret == nullptr && retry_count < 60) {
    if (retry_count == 0) {
      ut_print_timestamp(ib_stream);

      ib_logger(
        ib_stream,
        "  Error: cannot allocate"
        " %lu bytes of\n"
        "memory with malloc!"
        " Total allocated memory\n"
        "by InnoDB %lu bytes."
        " Operating system errno: %lu\n"
        "Check if you should"
        " increase the swap file or\n"
        "ulimits of your operating system.\n"
        "On FreeBSD check you"
        " have compiled the OS with\n"
        "a big enough maximum process size.\n"
        "Note that in most 32-bit"
        " computers the process\n"
        "memory space is limited"
        " to 2 GB or 4 GB.\n"
        "We keep retrying"
        " the allocation for 60 seconds...\n",
        (ulong)n,
        (ulong)ut_total_allocated_memory,
        (ulong)errno
      );
    }

    os_fast_mutex_unlock(&ut_list_mutex);

    /* Sleep for a second and retry the allocation; maybe this is
    just a temporary shortage of memory */

    os_thread_sleep(1000000);

    retry_count++;

    goto retry;
  }

  if (ret == nullptr) {
    os_fast_mutex_unlock(&ut_list_mutex);

    /* Make an intentional seg fault so that we get a stack trace */
    /* Intentional segfault on NetWare causes an abend. Avoid this
    by graceful exit handling in ut_a(). */

    if (assert_on_error) {
      ut_print_timestamp(ib_stream);

      ib_logger(
        ib_stream,
        "  We now intentionally"
        " generate a seg fault so that\n"
        "on Linux we get a stack trace.\n"
      );

      if (*ut_mem_null_ptr) {
        ut_mem_null_ptr = 0;
      }
    } else {
      return nullptr;
    }
  }

  if (set_to_zero) {
#ifdef UNIV_SET_MEM_TO_ZERO
    memset(ret, '\0', n + sizeof(ut_mem_block_t));
#endif
  }

  UNIV_MEM_ALLOC(ret, n + sizeof(ut_mem_block_t));

  ((ut_mem_block_t *)ret)->size = n + sizeof(ut_mem_block_t);
  ((ut_mem_block_t *)ret)->magic_n = UT_MEM_MAGIC_N;

  ut_total_allocated_memory += n + sizeof(ut_mem_block_t);

  UT_LIST_ADD_FIRST(mem_block_list, ut_mem_block_list, ((ut_mem_block_t *)ret));
  os_fast_mutex_unlock(&ut_list_mutex);

  return ((void *)((byte *)ret + sizeof(ut_mem_block_t)));
}

void *ut_malloc(ulint n) {
  return (ut_malloc_low(n, true, true));
}

bool ut_test_malloc(ulint n) {
  auto ret = malloc(n);

  if (ret == nullptr) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "  Error: cannot allocate"
      " %lu bytes of memory for\n"
      "a BLOB with malloc! Total allocated memory\n"
      "by InnoDB %lu bytes."
      " Operating system errno: %d\n"
      "Check if you should increase"
      " the swap file or\n"
      "ulimits of your operating system.\n"
      "On FreeBSD check you have"
      " compiled the OS with\n"
      "a big enough maximum process size.\n",
      (ulong)n,
      (ulong)ut_total_allocated_memory,
      (int)errno
    );
    return (false);
  }

  free(ret);

  return (true);
}

void ut_free(void *ptr) {
  ut_mem_block_t *block;

  if (!ptr) {
    return;
  } else if (likely(srv_use_sys_malloc)) {
    free(ptr);
    return;
  }

  block = (ut_mem_block_t *)((byte *)ptr - sizeof(ut_mem_block_t));

  os_fast_mutex_lock(&ut_list_mutex);

  ut_a(block->magic_n == UT_MEM_MAGIC_N);
  ut_a(ut_total_allocated_memory >= block->size);

  ut_total_allocated_memory -= block->size;

  UT_LIST_REMOVE(mem_block_list, ut_mem_block_list, block);
  free(block);

  os_fast_mutex_unlock(&ut_list_mutex);
}

void *ut_realloc(void *ptr, ulint size) {
  ut_mem_block_t *block;
  ulint old_size;
  ulint min_size;
  void *new_ptr;

  if (likely(srv_use_sys_malloc)) {
    return (realloc(ptr, size));
  }

  if (ptr == nullptr) {

    return (ut_malloc(size));
  }

  if (size == 0) {
    ut_free(ptr);

    return (nullptr);
  }

  block = (ut_mem_block_t *)((byte *)ptr - sizeof(ut_mem_block_t));

  ut_a(block->magic_n == UT_MEM_MAGIC_N);

  old_size = block->size - sizeof(ut_mem_block_t);

  if (size < old_size) {
    min_size = size;
  } else {
    min_size = old_size;
  }

  new_ptr = ut_malloc(size);

  if (new_ptr == nullptr) {

    return (nullptr);
  }

  /* Copy the old data from ptr */
  memcpy(new_ptr, ptr, min_size);

  ut_free(ptr);

  return (new_ptr);
}

void ut_free_all_mem() {
  ut_mem_block_t *block;

  /* If the sub-system hasn't been initialized, then ignore request. */
  if (!ut_mem_block_list_inited) {
    return;
  }

  os_fast_mutex_free(&ut_list_mutex);

  while ((block = UT_LIST_GET_FIRST(ut_mem_block_list))) {

    ut_a(block->magic_n == UT_MEM_MAGIC_N);
    ut_a(ut_total_allocated_memory >= block->size);

    ut_total_allocated_memory -= block->size;

    UT_LIST_REMOVE(mem_block_list, ut_mem_block_list, block);
    free(block);
  }

  if (ut_total_allocated_memory != 0) {
    ib_logger(
      ib_stream,
      "Warning: after shutdown"
      " total allocated memory is %lu\n",
      (ulong)ut_total_allocated_memory
    );
  }

  ut_mem_block_list_inited = false;
}

ulint ut_strlcpy(char *dst, const char *src, ulint size) {
  ulint src_size = strlen(src);

  if (size != 0) {
    ulint n = ut_min(src_size, size - 1);

    memcpy(dst, src, n);
    dst[n] = '\0';
  }

  return (src_size);
}

ulint ut_strlcpy_rev(char *dst, const char *src, ulint size) {
  ulint src_size = strlen(src);

  if (size != 0) {
    ulint n = ut_min(src_size, size - 1);

    memcpy(dst, src + src_size - n, n + 1);
  }

  return (src_size);
}

char *strcpyq(char *dest, char q, const char *src) {
  while (*src) {
    if ((*dest++ = *src++) == q) {
      *dest++ = q;
    }
  }

  return (dest);
}

char *memcpyq(char *dest, char q, const char *src, ulint len) {
  const char *srcend = src + len;

  while (src < srcend) {
    if ((*dest++ = *src++) == q) {
      *dest++ = q;
    }
  }

  return (dest);
}

ulint ut_strcount(const char *s1, const char *s2) {
  ulint count = 0;
  ulint len = strlen(s2);

  if (len == 0) {

    return (0);
  }

  for (;;) {
    s1 = strstr(s1, s2);

    if (!s1) {

      break;
    }

    count++;
    s1 += len;
  }

  return (count);
}

char *ut_strreplace(const char *str, const char *s1, const char *s2) {
  char *ptr;
  const char *str_end;
  ulint str_len = strlen(str);
  ulint s1_len = strlen(s1);
  ulint s2_len = strlen(s2);
  ulint count = 0;
  int len_delta = (int)s2_len - (int)s1_len;

  str_end = str + str_len;

  if (len_delta <= 0) {
    len_delta = 0;
  } else {
    count = ut_strcount(str, s1);
  }

  auto new_str = static_cast<char *>(mem_alloc(str_len + count * len_delta + 1));
  ptr = new_str;

  while (str) {
    const char *next = strstr(str, s1);

    if (!next) {
      next = str_end;
    }

    memcpy(ptr, str, next - str);
    ptr += next - str;

    if (next == str_end) {

      break;
    }

    memcpy(ptr, s2, s2_len);
    ptr += s2_len;

    str = next + s1_len;
  }

  *ptr = '\0';

  return (new_str);
}

#ifdef UNIV_COMPILE_TEST_FUNCS

void test_ut_str_sql_format() {
  char buf[128];
  ulint ret;

#define CALL_AND_TEST(str, str_len, buf, buf_size, ret_expected, buf_expected)                \
  do {                                                                                        \
    bool ok = true;                                                                           \
    memset(buf, 'x', 10);                                                                     \
    buf[10] = '\0';                                                                           \
    ib_logger(ib_stream, "TESTING \"%s\", %lu, %lu\n", str, (ulint)str_len, (ulint)buf_size); \
    ret = ut_str_sql_format(str, str_len, buf, buf_size);                                     \
    if (ret != ret_expected) {                                                                \
      ib_logger(ib_stream, "expected ret %lu, got %lu\n", (ulint)ret_expected, ret);          \
      ok = false;                                                                             \
    }                                                                                         \
    if (strcmp((char *)buf, buf_expected) != 0) {                                             \
      ib_logger(ib_stream, "expected buf \"%s\", got \"%s\"\n", buf_expected, buf);           \
      ok = false;                                                                             \
    }                                                                                         \
    if (ok) {                                                                                 \
      ib_logger(ib_stream, "OK: %lu, \"%s\"\n\n", (ulint)ret, buf);                           \
    } else {                                                                                  \
      return;                                                                                 \
    }                                                                                         \
  } while (0)

  CALL_AND_TEST("abcd", 4, buf, 0, 0, "xxxxxxxxxx");

  CALL_AND_TEST("abcd", 4, buf, 1, 1, "");

  CALL_AND_TEST("abcd", 4, buf, 2, 1, "");

  CALL_AND_TEST("abcd", 0, buf, 3, 3, "''");
  CALL_AND_TEST("abcd", 1, buf, 3, 1, "");
  CALL_AND_TEST("abcd", 2, buf, 3, 1, "");
  CALL_AND_TEST("abcd", 3, buf, 3, 1, "");
  CALL_AND_TEST("abcd", 4, buf, 3, 1, "");

  CALL_AND_TEST("abcd", 0, buf, 4, 3, "''");
  CALL_AND_TEST("abcd", 1, buf, 4, 4, "'a'");
  CALL_AND_TEST("abcd", 2, buf, 4, 4, "'a'");
  CALL_AND_TEST("abcd", 3, buf, 4, 4, "'a'");
  CALL_AND_TEST("abcd", 4, buf, 4, 4, "'a'");
  CALL_AND_TEST("abcde", 5, buf, 4, 4, "'a'");
  CALL_AND_TEST("'", 1, buf, 4, 3, "''");
  CALL_AND_TEST("''", 2, buf, 4, 3, "''");
  CALL_AND_TEST("a'", 2, buf, 4, 4, "'a'");
  CALL_AND_TEST("'a", 2, buf, 4, 3, "''");
  CALL_AND_TEST("ab", 2, buf, 4, 4, "'a'");

  CALL_AND_TEST("abcdef", 0, buf, 5, 3, "''");
  CALL_AND_TEST("abcdef", 1, buf, 5, 4, "'a'");
  CALL_AND_TEST("abcdef", 2, buf, 5, 5, "'ab'");
  CALL_AND_TEST("abcdef", 3, buf, 5, 5, "'ab'");
  CALL_AND_TEST("abcdef", 4, buf, 5, 5, "'ab'");
  CALL_AND_TEST("abcdef", 5, buf, 5, 5, "'ab'");
  CALL_AND_TEST("abcdef", 6, buf, 5, 5, "'ab'");
  CALL_AND_TEST("'", 1, buf, 5, 5, "''''");
  CALL_AND_TEST("''", 2, buf, 5, 5, "''''");
  CALL_AND_TEST("a'", 2, buf, 5, 4, "'a'");
  CALL_AND_TEST("'a", 2, buf, 5, 5, "''''");
  CALL_AND_TEST("ab", 2, buf, 5, 5, "'ab'");
  CALL_AND_TEST("abc", 3, buf, 5, 5, "'ab'");

  CALL_AND_TEST("ab", 2, buf, 6, 5, "'ab'");

  CALL_AND_TEST("a'b'c", 5, buf, 32, 10, "'a''b''c'");
  CALL_AND_TEST("a'b'c'", 6, buf, 32, 12, "'a''b''c'''");
}

#endif /* UNIV_COMPILE_TEST_FUNCS */
