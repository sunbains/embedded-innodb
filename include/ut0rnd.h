/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/ut0rnd.h
Random numbers and hashing

Created 1/20/1994 Heikki Tuuri
***********************************************************************/

#pragma once

#include "innodb0types.h"

#include "ut0byte.h"

/** The 'character code' for end of field or string (used
in folding records */
constexpr ulint UT_END_OF_FIELD = 257;

/** This is used to set the random number seed. */
inline void ut_rnd_set_seed(ulint seed); /*!< in: seed */

/** The following function generates a series of 'random' ulint integers.
@return	the next 'random' number */
inline ulint ut_rnd_gen_next_ulint(ulint rnd); /*!< in: the previous random number value */

/** The following function generates 'random' ulint integers which
enumerate the value space (let there be N of them) of ulint integers
in a pseudo-random fashion. Note that the same integer is repeated
always after N calls to the generator.
@return	the 'random' number */
inline ulint ut_rnd_gen_ulint(void);

/** Generates a random integer from a given interval.
@return	the 'random' number */
inline ulint ut_rnd_interval(
  ulint low, /*!< in: low limit; can generate also this value */
  ulint high
); /*!< in: high limit; can generate also this value */

/** Generates a random boolean value.
@return	the random value */
inline bool ut_rnd_gen_bool(void);

/** The following function generates a hash value for a ulint integer
to a hash table of size table_size, which should be a prime or some
random number to work reliably.
@return	hash value */
inline ulint ut_hash_ulint(
  ulint key, /*!< in: value to be hashed */
  ulint table_size
); /*!< in: hash table size */

/** Folds a pair of ulints.
@return	folded value */
inline ulint ut_fold_ulint_pair(
  ulint n1, /*!< in: ulint */
  ulint n2
) /*!< in: ulint */
  __attribute__((const));

/** Folds a uint64_t.
@return	folded value */
inline ulint ut_uint64_fold(uint64_t d) /*!< in: uint64_t */
  __attribute__((const));

/** Folds a character string ending in the null character.
@return	folded value */
inline ulint ut_fold_string(const char *str) /*!< in: null-terminated string */
  __attribute__((pure));

/** Folds a binary string.
@return	folded value */
inline ulint ut_fold_binary(
  const byte *str, /*!< in: string of bytes */
  ulint len
) /*!< in: length */
  __attribute__((pure));

/** Looks for a prime number slightly greater than the given argument.
The prime is chosen so that it is not near any power of 2.
@return	prime */
ulint ut_find_prime(ulint n) /*!< in: positive number > 100 */
  __attribute__((const));

constexpr ulint UT_HASH_RANDOM_MASK = 1463735687;
constexpr ulint UT_HASH_RANDOM_MASK2 = 1653893711;
constexpr ulint UT_RND1 = 151117737;
constexpr ulint UT_RND2 = 119785373;
constexpr ulint UT_RND3 = 85689495;
constexpr ulint UT_RND4 = 76595339;
constexpr ulint UT_SUM_RND2 = 98781234;
constexpr ulint UT_SUM_RND3 = 126792457;
constexpr ulint UT_SUM_RND4 = 63498502;
constexpr ulint UT_XOR_RND1 = 187678878;
constexpr ulint UT_XOR_RND2 = 143537923;

/** Seed value of ut_rnd_gen_ulint() */
extern ulint ut_rnd_ulint_counter;

/** This is used to set the random number seed. */
inline void ut_rnd_set_seed(ulint seed) /*!< in: seed */
{
  ut_rnd_ulint_counter = seed;
}

/** The following function generates a series of 'random' ulint integers.
@return	the next 'random' number */
inline ulint ut_rnd_gen_next_ulint(ulint rnd) /*!< in: the previous random number value */
{
  ulint n_bits;

  n_bits = 8 * sizeof(ulint);

  rnd = UT_RND2 * rnd + UT_SUM_RND3;
  rnd = UT_XOR_RND1 ^ rnd;
  rnd = (rnd << 20) + (rnd >> (n_bits - 20));
  rnd = UT_RND3 * rnd + UT_SUM_RND4;
  rnd = UT_XOR_RND2 ^ rnd;
  rnd = (rnd << 20) + (rnd >> (n_bits - 20));
  rnd = UT_RND1 * rnd + UT_SUM_RND2;

  return (rnd);
}

/** The following function generates 'random' ulint integers which
enumerate the value space of ulint integers in a pseudo random
fashion. Note that the same integer is repeated always after
2 to power 32 calls to the generator (if ulint is 32-bit).
@return	the 'random' number */
inline ulint ut_rnd_gen_ulint(void) {
  /* ulint n_bits = 8 * sizeof(ulint); */

  ut_rnd_ulint_counter = UT_RND1 * ut_rnd_ulint_counter + UT_RND2;

  return ut_rnd_gen_next_ulint(ut_rnd_ulint_counter);
}

/** Generates a random integer from a given interval.
@return	the 'random' number */
inline ulint ut_rnd_interval(
  ulint low, /*!< in: low limit; can generate also this value */
  ulint high
) /*!< in: high limit; can generate also this value */
{
  ulint rnd;

  ut_ad(high >= low);

  if (low == high) {

    return (low);
  }

  rnd = ut_rnd_gen_ulint();

  return (low + (rnd % (high - low + 1)));
}

/** Generates a random boolean value.
@return	the random value */
inline bool ut_rnd_gen_bool(void) {
  ulint x;

  x = ut_rnd_gen_ulint();

  if (((x >> 20) + (x >> 15)) & 1) {

    return (true);
  }

  return (false);
}

/** The following function generates a hash value for a ulint integer
to a hash table of size table_size, which should be a prime
or some random number for the hash table to work reliably.
@return	hash value */
inline ulint ut_hash_ulint(
  ulint key, /*!< in: value to be hashed */
  ulint table_size
) /*!< in: hash table size */
{
  ut_ad(table_size);
  key = key ^ UT_HASH_RANDOM_MASK2;

  return (key % table_size);
}

/** Folds a pair of ulints.
@return	folded value */
inline ulint ut_fold_ulint_pair(
  ulint n1, /*!< in: ulint */
  ulint n2
) /*!< in: ulint */
{
  return (((((n1 ^ n2 ^ UT_HASH_RANDOM_MASK2) << 8) + n1) ^ UT_HASH_RANDOM_MASK) + n2);
}

/** Folds a uint64_t integer.
@return	folded value */
inline ulint ut_uint64_fold(uint64_t d) /*!< in: uint64_t */
{
  return ut_fold_ulint_pair(ulint(d) & UINT32_MASK, ulint(d >> 32));
}

/** Folds a character string ending in the null character.
@return	folded value */
inline ulint ut_fold_string(const char *str) /*!< in: null-terminated string */
{
  ulint fold = 0;

  ut_ad(str);

  while (*str != '\0') {
    fold = ut_fold_ulint_pair(fold, (ulint)(*str));
    str++;
  }

  return (fold);
}

/** Folds a binary string.
@return	folded value */
inline ulint ut_fold_binary(
  const byte *str, /*!< in: string of bytes */
  ulint len
) /*!< in: length */
{
  const byte *str_end = str + len;
  ulint fold = 0;

  ut_ad(str || !len);

  while (str < str_end) {
    fold = ut_fold_ulint_pair(fold, (ulint)(*str));

    str++;
  }

  return (fold);
}
