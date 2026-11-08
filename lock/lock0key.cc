/****************************************************************************
Copyright (c) 2026 Sunny Bains. All rights reserved.

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

/** @file lock/lock0key.cc
The fingerprint of a key for its record locks. Two keys that compare equal have one fingerprint: the
bytes go in as the comparison sees them (rem0cmp.cc), without the padding it ignores, and a field that
it compares as a whole value (floats, decimals, client types) doesn't go in at all. So the comparison
never runs on a lock: the fingerprint is all there is, and a collision is only a lock shared. */

#include "lock0lock.h"

#include "data0data.h"
#include "data0type.h"
#include "dict0types.h"
#include "rem0rec.h"

namespace lock {

namespace {

constexpr uint64_t SEED = 0x9E3779B97F4A7C15ULL;

/** A NULL is a value of its own. */
constexpr uint64_t NULL_FIELD = 0xD6E8FEB86659FD93ULL;

/** The fingerprint of the end of an index, a fence after its last key. */
constexpr uint64_t END_FOLD = ~uint64_t{};

[[nodiscard]] inline uint64_t mix(uint64_t h, uint64_t v) noexcept {
  h ^= v + SEED + (h << 6) + (h >> 2);
  return h * 0xFF51AFD7ED558CCDULL;
}

/** @return true if the comparison of the type looks at the whole value, not its bytes. */
[[nodiscard]] inline bool whole_value(ulint mtype, ulint prtype) noexcept {
  return mtype >= DATA_FLOAT ||
         (mtype == DATA_BLOB && (prtype & DATA_BINARY_TYPE) == 0 && dtype_get_charset_coll(prtype) != DATA_CLIENT_LATIN1_SWEDISH_CHARSET_COLL);
}

/** Add a field, as the comparison sees it. */
[[nodiscard]] uint64_t add_field(uint64_t h, ulint mtype, ulint prtype, const byte *data, ulint len) noexcept {
  if (len == UNIV_SQL_NULL) {
    return mix(h, NULL_FIELD);
  }

  if (whole_value(mtype, prtype)) {
    /* Nothing of it: a collision is all it can cost. */
    return mix(h, 1);
  }

  /* What the shorter of two values is padded with: the same value without it. */
  if (const auto pad = dtype_get_pad_char(mtype, prtype); pad != ULINT_UNDEFINED) {
    while (len > 0 && data[len - 1] == byte(pad)) {
      --len;
    }
  }

  ulint i{};

  for (; i + 8 <= len; i += 8) {
    uint64_t w;

    memcpy(&w, data + i, sizeof(w));
    h = mix(h, w);
  }

  uint64_t rest{};

  for (ulint j{}; i + j < len; ++j) {
    rest |= uint64_t(data[i + j]) << (8 * j);
  }

  /* The length: "a" and "a\0" differ where there's no padding. */
  return mix(mix(h, rest), len);
}

[[nodiscard]] inline LockKey make(const Index *index, uint64_t h) noexcept {
  h = mix(h, index->m_id);

  /* The end is a fold of its own. */
  return LockKey{h == END_FOLD ? h - 1 : h};
}

}  // namespace

LockKey key_of(const Index *index, const Rec rec, const ulint *offsets) noexcept {
  const auto n = index->get_n_unique_in_tree();
  const RecOffs offs(const_cast<ulint *>(offsets));
  uint64_t h{SEED};

  for (ulint i{}; i < n; ++i) {
    const auto col = index->get_nth_field(i)->get_col();
    ulint len;
    const auto data = rec.get_nth_field(offs, i, &len);

    h = add_field(h, col->mtype, col->prtype, data.get(), len);
  }

  return make(index, h);
}

LockKey key_of(const Index *index, const DTuple *tuple) noexcept {
  const auto n = std::min(index->get_n_unique_in_tree(), dtuple_get_n_fields(tuple));
  uint64_t h{SEED};

  for (ulint i{}; i < n; ++i) {
    const auto field = dtuple_get_nth_field(tuple, i);
    const auto type = dfield_get_type(field);

    h = add_field(h, type->mtype, type->prtype, static_cast<const byte *>(dfield_get_data(field)), dfield_get_len(field));
  }

  return make(index, h);
}

LockKey end_of(const Index *index) noexcept { return LockKey{mix(END_FOLD, index->m_id) | 1}; }

}  // namespace lock
