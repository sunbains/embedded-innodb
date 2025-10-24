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

    ret = ::gettimeofday(&tv, nullptr);

    if (ret == -1) {
      errno_gettimeofday = errno;
      log_err("  gettimeofday(): ", strerror(errno_gettimeofday));
      usleep(100000);
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

  ::gettimeofday(&tv, nullptr);

  auto us = (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;

  if (tloc != nullptr) {
    *tloc = us;
  }

  return us;
}

ulint ut_time_ms() noexcept {
  struct timeval tv;

  ::gettimeofday(&tv, nullptr);

  return (ulint)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

double ut_difftime(ib_time_t time2, ib_time_t time1) noexcept {
  return ::difftime(time2, time1);
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

ulint ut_2_power_up(ulint n) noexcept {
  ulint res = 1;

  ut_ad(n > 0);

  while (res < n) {
    res = res * 2;
  }

  return res;
}
