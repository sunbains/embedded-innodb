/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.
Copyright (c) 2009, Sun Microsystems, Inc.
Copyright (c) 2024 Sunny Bains. All rights reserved.

Portions of this file contain modifications contributed and copyrighted by
Sun Microsystems, Inc. Those modifications are gratefully acknowledged and
are described briefly in the InnoDB documentation. The contributions by
Sun Microsystems are incorporated with their permission, and subject to the
conditions contained in the file COPYING.Sun_Microsystems.

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

/** @file ut/ut0ut.c
Various utilities for Innobase.

Created 5/11/1994 Heikki Tuuri
********************************************************************/

#include <innodb0types.h>

#include <format>
#include <sstream>

#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <cstdarg>

#include "api0ucode.h"
#include "trx0trx.h"
#include "ut0ut.h"

static bool ut_always_false = false;

#include <sys/time.h>

ulint ut_get_high32(ulint a) noexcept {
  auto i = (int64_t)a;

  i = i >> 32;

  return (ulint)i;
}

ib_time_t ut_time() noexcept {
  return time(nullptr);
}

int ut_usectime(ulint *sec, ulint *ms) noexcept {
  int ret;
  struct timeval tv;
  int errno_gettimeofday;

  for (ulint i = 0; i < 10; i++) {

    ret = gettimeofday(&tv, nullptr);

    if (ret == -1) {
      errno_gettimeofday = errno;
      ut_print_timestamp(ib_stream);
      ib_logger(ib_stream, "  gettimeofday(): %s\n", strerror(errno_gettimeofday));
      os_thread_sleep(100000); /* 0.1 sec */
      errno = errno_gettimeofday;
    } else {
      break;
    }
  }

  if (ret != -1) {
    *sec = (ulint)tv.tv_sec;
    *ms = (ulint)tv.tv_usec;
  }

  return ret;
}

uint64_t ut_time_us(uint64_t *tloc) noexcept {
  struct timeval tv;

  gettimeofday(&tv, nullptr);

  auto us = (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;

  if (tloc != nullptr) {
    *tloc = us;
  }

  return us;
}

ulint ut_time_ms() noexcept {
  struct timeval tv;

  gettimeofday(&tv, nullptr);

  return (ulint)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

double ut_difftime(ib_time_t time2, ib_time_t time1) noexcept {
  return difftime(time2, time1);
}

void ut_print_timestamp(ib_stream_t ib_stream) noexcept {
  time_t tm;
  struct tm cal_tm;
  struct tm *cal_tm_ptr;

  time(&tm);

#ifdef HAVE_LOCALTIME_R
  localtime_r(&tm, &cal_tm);
  cal_tm_ptr = &cal_tm;
#else  /* HAVE_LOCALTIME_R */
  cal_tm_ptr = localtime(&tm);
#endif /* HAVE_LOCALTIME_R */

  ib_logger(
    ib_stream,
    "%02d%02d%02d %2d:%02d:%02d",
    cal_tm_ptr->tm_year % 100,
    cal_tm_ptr->tm_mon + 1,
    cal_tm_ptr->tm_mday,
    cal_tm_ptr->tm_hour,
    cal_tm_ptr->tm_min,
    cal_tm_ptr->tm_sec
  );
}

void ut_sprintf_timestamp(char *buf) noexcept {
  struct tm cal_tm;
  struct tm *cal_tm_ptr;
  time_t tm;

  time(&tm);

#ifdef HAVE_LOCALTIME_R
  localtime_r(&tm, &cal_tm);
  cal_tm_ptr = &cal_tm;
#else  /* HAVE_LOCALTIME_R */
  cal_tm_ptr = localtime(&tm);
#endif /* HAVE_LOCALTIME_R */

  sprintf(
    buf,
    "%02d%02d%02d %2d:%02d:%02d",
    cal_tm_ptr->tm_year % 100,
    cal_tm_ptr->tm_mon + 1,
    cal_tm_ptr->tm_mday,
    cal_tm_ptr->tm_hour,
    cal_tm_ptr->tm_min,
    cal_tm_ptr->tm_sec
  );
}

ulint ut_delay(ulint delay) noexcept {
  ulint i, j;

  j = 0;

  for (i = 0; i < delay * 50; i++) {
    j += i;
    UT_RELAX_CPU();
  }

  if (ut_always_false) {
    ut_always_false = (bool)j;
  }

  return j;
}

std::ostream &buf_to_hex_string(std::ostream &o, const void *buf, ulint len) noexcept {
  UNIV_MEM_ASSERT_RW(buf, len);

  o << " buf = { len: " << len << ",\nhex = { ";

  auto data = reinterpret_cast<const byte *>(buf);

  for (uint i = 0; i < len; i++, ++data) {
    o << std::format("{:#x}", *data);
  }

  o << " },\nasc = { ";

  data = reinterpret_cast<const byte *>(buf);

  for (ulint i = 0; i < len; i++, ++data) {
    o << std::format("{}", *data);
  }

  o << "} }";

  return o;
}

void log_warn_buf(const void *buf, ulint len) noexcept {
  std::ostringstream os{};

  buf_to_hex_string(os, buf, len);

  log_warn(os.str().c_str());
}

ulint ut_2_power_up(ulint n) noexcept {
  ulint res = 1;

  ut_ad(n > 0);

  while (res < n) {
    res = res * 2;
  }

  return res;
}

void ut_print_name(const std::string &name) noexcept {
  log_info(std::format("'{}'", name));
}

void ut_print_filename(const std::string &name) noexcept {
  ut_print_name(name);
}

void ut_print_buf(const void *buf, ulint len) noexcept {
  std::ostringstream os{};
  buf_to_hex_string(os, buf, len);
  log_info(os.str());
}

int ut_snprintf(char *str, size_t size, const char *format, ...) noexcept {
  va_list ap;
  va_start(ap, format);
  int result = vsnprintf(str, size, format, ap);
  va_end(ap);
  return result;
}

ulint ut_format_name(char *buf, ulint buf_size, const char *name) noexcept {
  if (buf_size == 0) {
    return 0;
  }

  const char *slash = strchr(name, '/');
  if (slash == nullptr) {
    // No slash, just quote the name
    return snprintf(buf, buf_size, "\"%s\"", name);
  } else {
    // Has slash, format as "database"."table"
    const char *table = slash + 1;
    return snprintf(buf, buf_size, "\"%.*s\".\"%s\"", (int)(slash - name), name, table);
  }
}

size_t ut_strlcat(char *dst, const char *src, size_t size) noexcept {
  size_t dst_len = strlen(dst);
  size_t src_len = strlen(src);

  if (dst_len >= size) {
    return size + src_len;
  }

  size_t copy_len = size - dst_len - 1;
  if (src_len < copy_len) {
    copy_len = src_len;
  }

  memcpy(dst + dst_len, src, copy_len);
  dst[dst_len + copy_len] = '\0';

  return dst_len + src_len;
}

size_t ut_strlen(const char *str) noexcept {
  return strlen(str);
}

int ut_strcmp(const char *str1, const char *str2) noexcept {
  return strcmp(str1, str2);
}

int ut_strncmp(const char *str1, const char *str2, size_t n) noexcept {
  return strncmp(str1, str2, n);
}

char *ut_strncpy(char *dst, const char *src, size_t n) noexcept {
  strncpy(dst, src, n);
  dst[n - 1] = '\0';  // Ensure null termination
  return dst;
}
