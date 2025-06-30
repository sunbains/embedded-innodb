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

/** @file include/ut0ut.h
Various utilities

Created 1/20/1994 Heikki Tuuri
***********************************************************************/

#pragma once

#include <ctype.h>
#include <time.h>
#include <algorithm>

#include "os0sync.h"
#include "ut0dbg.h"
#include "ut0logger.h"

/** Index name prefix in fast index creation */
#define TEMP_INDEX_PREFIX '\377'
/** Index name prefix in fast index creation, as a string constant */
#define TEMP_INDEX_PREFIX_STR "\377"

/** Time stamp */
using ib_time_t = time_t;

// FIXME: Use proper C++ streams
#define ib_logger(s, f, ...)                       \
  do {                                             \
    ut_a(s != nullptr);                            \
    std::fprintf(s, f __VA_OPT__(, ) __VA_ARGS__); \
  } while (false)

#if defined(HAVE_IB_PAUSE_INSTRUCTION)
/* According to the gcc info page, asm volatile means that the
instruction has important side-effects and must not be removed.
Also asm volatile may trigger a memory barrier (spilling all registers
to memory). */
#define UT_RELAX_CPU() __asm__ __volatile__("pause")
#else
/** Avoid warning for an empty statement */
#define UT_RELAX_CPU() ((void)0)
#endif

/** Delays execution for at most max_wait_us microseconds or returns earlier
if cond becomes true.
@param cond		in: condition to wait for; evaluated every 2 ms
@param max_wait_us	in: maximum delay to wait, in microseconds */
#define UT_WAIT_FOR(cond, max_wait_us)                                  \
  do {                                                                  \
    uint64_t start_us;                                                  \
    start_us = ut_time_us(nullptr);                                     \
    while (!(cond) && ut_time_us(nullptr) - start_us < (max_wait_us)) { \
                                                                        \
      os_thread_sleep(2000 /* 2 ms */);                                 \
    }                                                                   \
  } while (0)

/** Gets the high 32 bits in a ulint. That is makes a shift >> 32,
 * but since there seem to be compiler bugs in both gcc and Visual C++,
 * we do this by a special conversion.
 * @param[in] a ulint to get the high 32 bits of
 * @return	a >> 32 */
ulint ut_get_high32(ulint a) noexcept;

/** Determines if a number is zero or a power of two.
 * @param[in] n number
 * @return		nonzero if n is zero or a power of two; zero otherwise */
#define ut_is_2pow(n) likely(!((n) & ((n) - 1)))

/** Calculates fast the remainder of n/m when m is a power of two.
 * @param[in] n numerator
 * @param[in] m denominator, must be a power of two
 * @return		the remainder of n/m */
#define ut_2pow_remainder(n, m) ((n) & ((m) - 1))

/** Calculates the biggest multiple of m that is not bigger than n
 * when m is a power of two.  In other words, rounds n down to m * k.
 * @param[in] n number to round down
 * @param[in] m alignment, must be a power of two
 * @return		n rounded down to the biggest possible integer multiple of m */
#define ut_2pow_round(n, m) ((n) & ~((m) - 1))

/** Align a number down to a multiple of a power of two.
 * @param[in] n number to round down
 * @param[in] m alignment, must be a power of two
 * @return		n rounded down to the biggest possible integer multiple of m */
#define ut_calc_align_down(n, m) ut_2pow_round(n, m)

/** Calculates the smallest multiple of m that is not smaller than n
 * when m is a power of two.  In other words, rounds n up to m * k.
 * @param[in] n number to round up
 * @param[in] m alignment, must be a power of two
 * @return		n rounded up to the smallest possible integer multiple of m */
#define ut_calc_align(n, m) (((n) + ((m) - 1)) & ~((m) - 1))

/** Calculates fast the number rounded up to the nearest power of 2.
 *
 * @param[in] n Number to round up (must not be 0)
 * @return First power of 2 which is >= n */
ulint ut_2_power_up(ulint n) noexcept __attribute__((const));

/** Determine how many bytes (groups of 8 bits) are needed to
store the given number of bits.
 * @param[in] b bits
 * @return		number of bytes (octets) needed to represent b */
#define UT_BITS_IN_BYTES(b) (((b) + 7) / 8)

/** Returns system time. We do not specify the format of the time returned:
 * the only way to manipulate it is to use the function ut_difftime.
 * @return	system time */
ib_time_t ut_time() noexcept;

/** Returns system time.
 * Upon successful completion, the value 0 is returned; otherwise the
 * value -1 is returned and the global variable errno is set to indicate the
 * error.
 *
 * @param[out] sec Seconds since the Epoch
 * @param[out] ms Microseconds since the Epoch+*sec
 * @return 0 on success, -1 otherwise */
int ut_usectime(ulint *sec, ulint *ms) noexcept;

/** Returns the number of microseconds since epoch. Similar to
 * time(3), the return value is also stored in *tloc, provided
 * that tloc is non-NULL.
 *
 * @param[out] tloc If non-NULL, microseconds since epoch will be stored here
 * @return microseconds since epoch */
uint64_t ut_time_us(uint64_t *tloc) noexcept;

/** Returns the number of milliseconds since some epoch. The
 * value may wrap around. It should only be used for heuristic
 * purposes.
 *
 * @return milliseconds since epoch */
ulint ut_time_ms() noexcept;

/** Returns the difference of two times in seconds.
 *
 * @param[in] time2 Second time value
 * @param[in] time1 First time value
 * @return time2 - time1 expressed in seconds */
double ut_difftime(ib_time_t time2, ib_time_t time1) noexcept;

/** Prints a timestamp to a file.
 *
 * @param[in] ib_stream File where to print the timestamp */
void ut_print_timestamp(ib_stream_t ib_stream) noexcept;

/** Sprintfs a timestamp to a buffer, 13..14 chars plus terminating NUL.
 *
 * @param[out] buf Buffer where to sprintf the timestamp */
void ut_sprintf_timestamp(char *buf) noexcept;

/** Runs an idle loop on CPU. The argument gives the desired delay
 * in microseconds on 100 MHz Pentium + Visual C++.
 *
 * @param[in] delay Delay in microseconds on 100 MHz Pentium
 * @return dummy value */
ulint ut_delay(ulint delay) noexcept;

/** Prints the contents of a memory buffer in hex and ascii as a warning.
 *
 * @param[in] buf Memory buffer to print
 * @param[in] len Length of the buffer */
void log_warn_buf(const void *buf, ulint len) noexcept;

/** Prints the contents of a memory buffer in hex and ascii.
 *
 * @param[in,out] o Output stream to write to
 * @param[in] buf Memory buffer to print
 * @param[in] len Length of the buffer
 * @return Output stream */
std::ostream &buf_to_hex_string(std::ostream &o, const void *buf, ulint len) noexcept;

/** Outputs a NUL-terminated file name, quoted with apostrophes.
 *
 * @param[in] name Name to print */
void ut_print_filename(const std::string &name) noexcept;

/** Outputs a fixed-length string, quoted as an SQL identifier.
 * If the string contains a slash '/', the string will be
 * output as two identifiers separated by a period (.),
 * e.g. "database"."table".
 *
 * @param[in] name Name to print */
void ut_print_name(const std::string &name) noexcept;

/** Prints the contents of a memory buffer in hex and ascii.
 *
 * @param[in] buf Memory buffer to print
 * @param[in] len Length of the buffer */
void ut_print_buf(const void *buf, ulint len) noexcept;

/** A wrapper for snprintf(3), formatted output conversion into
 * a buffer.
 *
 * @param[out] str Buffer to write to
 * @param[in] size Size of the buffer
 * @param[in] format Format string
 * @param[in] ... Arguments for the format string
 * @return Number of characters that would have been written if size had been sufficiently large */
int ut_snprintf(char *str, size_t size, const char *format, ...) noexcept;

/** Format a table name, quoted as an SQL identifier.
 * If the name contains a slash '/', the result will contain two
 * identifiers separated by a period (.), e.g. "database"."table".
 *
 * @param[out] buf Buffer to write to
 * @param[in] buf_size Size of the buffer
 * @param[in] name Name to format
 * @return Number of characters written to buf */
ulint ut_format_name(char *buf, ulint buf_size, const char *name) noexcept;

// FIXME: Use logger
#define ib_stream stderr

/** Calculates the maximum of two ulints.
 * @param[in] n1 First number
 * @param[in] n2 Second number
 * @return	maximum */
inline ulint ut_max(ulint n1, ulint n2) noexcept {
  return ((n1 <= n2) ? n2 : n1);
}

/** Calculates minimum of two ulint-pairs.
 * @param[out] a more significant part of minimum
 * @param[out] b less significant part of minimum
 * @param[in] a1 more significant part of first pair
 * @param[in] b1 less significant part of first pair
 * @param[in] a2 more significant part of second pair
 * @param[in] b2 less significant part of second pair */
inline void ut_pair_min(ulint *a, ulint *b, ulint a1, ulint b1, ulint a2, ulint b2) noexcept {
  if (a1 == a2) {
    *a = a1;
    *b = std::min(b1, b2);
  } else if (a1 < a2) {
    *a = a1;
    *b = b1;
  } else {
    *a = a2;
    *b = b2;
  }
}

/** Compares two ulints.
 * @param[in] a first ulint
 * @param[in] b second ulint
 * @return	1 if a > b, 0 if a == b, -1 if a < b */
inline int ut_ulint_cmp(ulint a, ulint b) noexcept {
  if (a < b) {
    return (-1);
  } else if (a == b) {
    return (0);
  } else {
    return (1);
  }
}

/** Compares two pairs of ulints.
 * @param[in] a1 more significant part of first pair
 * @param[in] a2 less significant part of first pair
 * @param[in] b1 more significant part of second pair
 * @param[in] b2 less significant part of second pair
 * @return	-1 if a < b, 0 if a == b, 1 if a > b */
inline int ut_pair_cmp(ulint a1, ulint a2, ulint b1, ulint b2) noexcept {
  if (a1 > b1) {
    return (1);
  } else if (a1 < b1) {
    return (-1);
  } else if (a2 > b2) {
    return (1);
  } else if (a2 < b2) {
    return (-1);
  } else {
    return (0);
  }
}

/** Calculates fast the 2-logarithm of a number, rounded upward to an
 * integer.
 *
 * @param[in] n number
 * @return	logarithm in the base 2, rounded upward */
inline ulint ut_2_log(ulint n) noexcept {
  ulint res{};

  ut_ad(n > 0);

  --n;

  for (;;) {
    n >>= 1;

    if (n == 0) {
      break;
    }

    ++res;
  }

  return res + 1;
}

/** Calculates 2 to power n.
 *
 * @param[in] n number
 * @return	2 to power n */
inline ulint ut_2_exp(ulint n) noexcept {
  return (ulint)1 << n;
}

/** Safe version of strncpy(3). The result is always NUL-terminated.
 *
 * @param[out] dst Destination buffer
 * @param[in] src Source string
 * @param[in] size Size of the destination buffer
 * @return Length of the source string */
size_t ut_strlcpy(char *dst, const char *src, size_t size) noexcept;

/** Safe version of strncat(3). The result is always NUL-terminated.
 *
 * @param[out] dst Destination buffer
 * @param[in] src Source string
 * @param[in] size Size of the destination buffer
 * @return Length of the destination string */
size_t ut_strlcat(char *dst, const char *src, size_t size) noexcept;

/** Safe version of strlen(3). The result is always NUL-terminated.
 *
 * @param[in] str String to get length of
 * @return Length of the string */
size_t ut_strlen(const char *str) noexcept;

/** Safe version of strcmp(3). The result is always NUL-terminated.
 *
 * @param[in] str1 First string to compare
 * @param[in] str2 Second string to compare
 * @return < 0 if str1 < str2, 0 if str1 == str2, > 0 if str1 > str2 */
int ut_strcmp(const char *str1, const char *str2) noexcept;

/** Safe version of strncmp(3). The result is always NUL-terminated.
 *
 * @param[in] str1 First string to compare
 * @param[in] str2 Second string to compare
 * @param[in] n Maximum number of characters to compare
 * @return < 0 if str1 < str2, 0 if str1 == str2, > 0 if str1 > str2 */
int ut_strncmp(const char *str1, const char *str2, size_t n) noexcept;

/** Safe version of strncpy(3). The result is always NUL-terminated.
 *
 * @param[out] dst Destination buffer
 * @param[in] src Source string
 * @param[in] n Maximum number of characters to copy
 * @return Pointer to the destination string */
char *ut_strncpy(char *dst, const char *src, size_t n) noexcept;
