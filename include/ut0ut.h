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

#include "os0sync.h"
#include "ut0dbg.h"
#include "ut0logger.h"

/** Index name prefix in fast index creation */
#define TEMP_INDEX_PREFIX '\377'
/** Index name prefix in fast index creation, as a string constant */
#define TEMP_INDEX_PREFIX_STR "\377"

/** Time stamp */
typedef time_t ib_time_t;

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
#define UT_WAIT_FOR(cond, max_wait_us)                               \
  do {                                                               \
    uint64_t start_us;                                               \
    start_us = ut_time_us(nullptr);                                     \
    while (!(cond) && ut_time_us(nullptr) - start_us < (max_wait_us)) { \
                                                                     \
      os_thread_sleep(2000 /* 2 ms */);                              \
    }                                                                \
  } while (0)

/** Gets the high 32 bits in a ulint. That is makes a shift >> 32,
but since there seem to be compiler bugs in both gcc and Visual C++,
we do this by a special conversion.
@return	a >> 32 */
ulint ut_get_high32(ulint a); /*!< in: ulint */

/** Calculates the minimum of two ulints.
@return	minimum */
ulint ut_min(
  ulint n1, /*!< in: first number */
  ulint n2
); /*!< in: second number */

/** Determines if a number is zero or a power of two.
@param n	in: number
@return		nonzero if n is zero or a power of two; zero otherwise */
#define ut_is_2pow(n) likely(!((n) & ((n)-1)))

/** Calculates fast the remainder of n/m when m is a power of two.
@param n	in: numerator
@param m	in: denominator, must be a power of two
@return		the remainder of n/m */
#define ut_2pow_remainder(n, m) ((n) & ((m)-1))

/** Calculates the biggest multiple of m that is not bigger than n
when m is a power of two.  In other words, rounds n down to m * k.
@param n	in: number to round down
@param m	in: alignment, must be a power of two
@return		n rounded down to the biggest possible integer multiple of m */
#define ut_2pow_round(n, m) ((n) & ~((m)-1))

/** Align a number down to a multiple of a power of two.
@param n	in: number to round down
@param m	in: alignment, must be a power of two
@return		n rounded down to the biggest possible integer multiple of m */
#define ut_calc_align_down(n, m) ut_2pow_round(n, m)

/** Calculates the smallest multiple of m that is not smaller than n
when m is a power of two.  In other words, rounds n up to m * k.
@param n	in: number to round up
@param m	in: alignment, must be a power of two
@return		n rounded up to the smallest possible integer multiple of m */
#define ut_calc_align(n, m) (((n) + ((m)-1)) & ~((m)-1))

/** Calculates fast the number rounded up to the nearest power of 2.
@return        first power of 2 which is >= n */
ulint ut_2_power_up(ulint n) /*!< in: number != 0 */
  __attribute__((const));

/** Determine how many bytes (groups of 8 bits) are needed to
store the given number of bits.
@param b	in: bits
@return		number of bytes (octets) needed to represent b */
#define UT_BITS_IN_BYTES(b) (((b) + 7) / 8)

/** Returns system time. We do not specify the format of the time returned:
the only way to manipulate it is to use the function ut_difftime.
@return	system time */
ib_time_t ut_time(void);

/** Returns system time.
Upon successful completion, the value 0 is returned; otherwise the
value -1 is returned and the global variable errno is set to indicate the
error.
@return	0 on success, -1 otherwise */
int ut_usectime(
  ulint *sec, /*!< out: seconds since the Epoch */
  ulint *ms
); /*!< out: microseconds since the Epoch+*sec */

/** Returns the number of microseconds since epoch. Similar to
time(3), the return value is also stored in *tloc, provided
that tloc is non-NULL.
@return	us since epoch */
uint64_t ut_time_us(uint64_t *tloc); /*!< out: us since epoch, if non-NULL */

/** Returns the number of milliseconds since some epoch.  The
value may wrap around.  It should only be used for heuristic
purposes.
@return	ms since epoch */
ulint ut_time_ms(void);

/** Returns the difference of two times in seconds.
@return	time2 - time1 expressed in seconds */
double ut_difftime(
  ib_time_t time2, /*!< in: time */
  ib_time_t time1
); /*!< in: time */

/** Prints a timestamp to a file. */
void ut_print_timestamp(ib_stream_t ib_stream); /*!< in: file where to print */

/** Sprintfs a timestamp to a buffer, 13..14 chars plus terminating NUL. */
void ut_sprintf_timestamp(char *buf); /*!< in: buffer where to sprintf */

/** Runs an idle loop on CPU. The argument gives the desired delay
in microseconds on 100 MHz Pentium + Visual C++.
@return	dummy value */
ulint ut_delay(ulint delay); /*!< in: delay in microseconds on 100 MHz Pentium */

/**
 * Prints the contents of a memory buffer in hex and ascii as a warning
 *
 * @param[in] buf memory buffer
 * @param[in] len length of the buffer
 */
void log_warn_buf(const void *buf, ulint len) noexcept;

/** Prints the contents of a memory buffer in hex and ascii.
 * 
 * @param[in,out] o output stream
 * @param[in] buf memory buffer
 * @param[in] len length of the buffer
 * @return output stream */
std::ostream &buf_to_hex_string(std::ostream &o, const void *buf, ulint len); 

/** Outputs a NUL-terminated file name, quoted with apostrophes. */
void ut_print_filename(const std::string &name) noexcept; /*!< in: name to print */

/* Forward declaration of transaction handle */
struct trx_t;

/** Outputs a fixed-length string, quoted as an SQL identifier.
If the string contains a slash '/', the string will be
output as two identifiers separated by a period (.),
as in SQL database_name.identifier. */
void ut_print_name(const std::string &name) noexcept;

/** A wrapper for snprintf(3), formatted output conversion into
a limited buffer. */
#define ut_snprintf snprintf

// FIXME: Use logger
#define ib_stream stderr

/** Calculates the minimum of two ulints.
@return	minimum */
inline ulint ut_min(
  ulint n1, /*!< in: first number */
  ulint n2
) /*!< in: second number */
{
  return ((n1 <= n2) ? n1 : n2);
}

/** Calculates the maximum of two ulints.
@return	maximum */
inline ulint ut_max(
  ulint n1, /*!< in: first number */
  ulint n2
) /*!< in: second number */
{
  return ((n1 <= n2) ? n2 : n1);
}

/** Calculates minimum of two ulint-pairs. */
inline void ut_pair_min(
  ulint *a, /*!< out: more significant part of minimum */
  ulint *b, /*!< out: less significant part of minimum */
  ulint a1, /*!< in: more significant part of first pair */
  ulint b1, /*!< in: less significant part of first pair */
  ulint a2, /*!< in: more significant part of second pair */
  ulint b2
) /*!< in: less significant part of second pair */
{
  if (a1 == a2) {
    *a = a1;
    *b = ut_min(b1, b2);
  } else if (a1 < a2) {
    *a = a1;
    *b = b1;
  } else {
    *a = a2;
    *b = b2;
  }
}

/** Compares two ulints.
@return	1 if a > b, 0 if a == b, -1 if a < b */
inline int ut_ulint_cmp(
  ulint a, /*!< in: ulint */
  ulint b
) /*!< in: ulint */
{
  if (a < b) {
    return (-1);
  } else if (a == b) {
    return (0);
  } else {
    return (1);
  }
}

/** Compares two pairs of ulints.
@return	-1 if a < b, 0 if a == b, 1 if a > b */
inline int ut_pair_cmp(
  ulint a1, /*!< in: more significant part of first pair */
  ulint a2, /*!< in: less significant part of first pair */
  ulint b1, /*!< in: more significant part of second pair */
  ulint b2
) /*!< in: less significant part of second pair */
{
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
integer.
@return	logarithm in the base 2, rounded upward */
inline ulint ut_2_log(ulint n) /*!< in: number != 0 */
{
  ulint res;

  res = 0;

  ut_ad(n > 0);

  n = n - 1;

  for (;;) {
    n = n / 2;

    if (n == 0) {
      break;
    }

    res++;
  }

  return (res + 1);
}

/** Calculates 2 to power n.
@return	2 to power n */
inline ulint ut_2_exp(ulint n) /*!< in: number */
{
  return ((ulint)1 << n);
}
