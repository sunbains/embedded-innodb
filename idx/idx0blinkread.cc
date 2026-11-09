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

/** @file idx/idx0blinkread.cc
Reading the pages of a B-link tree with no latch: every function here takes a frame that can change
under it at any moment, stays inside the page whatever it finds on it, and says when it can't make
sense of it. The version decides afterwards whether what it saw means anything: read_stable(). */

#include "idx0blinkint.h"

#include "btr0btr.h"
#include "btr0cur.h"
#include "mtr0mtr.h"
#include "rem0cmp.h"
#include "rem0rec.h"
#include "srv0sys.h"

namespace blink {

namespace {

/** A record of a page that nothing holds still. Everything it reads is inside the page whatever
the bytes are, and none of it asserts on them: what it returns means something only if the version
of the block validates after. */
struct RawRec {
  /** @return false if what's at offs can't be a record. */
  [[nodiscard]] bool open(const page_t *page, ulint offs) noexcept {
    if (offs < PAGE_DATA + REC_N_EXTRA_BYTES || offs >= UNIV_PAGE_SIZE - PAGE_DIR) {
      return false;
    }

    m_rec = page + offs;
    m_room = UNIV_PAGE_SIZE - PAGE_DIR - offs;
    m_n_fields = (mach_read_from_2(m_rec - REC_N_FIELDS) & REC_N_FIELDS_MASK) >> REC_N_FIELDS_SHIFT;
    m_short = (mach_read_from_1(m_rec - REC_SHORT) & REC_SHORT_MASK) != 0;

    return m_n_fields > 0 && REC_N_EXTRA_BYTES + m_n_fields * (m_short ? 1 : 2) <= offs;
  }

  /** @return false if field n can't be what it says. */
  [[nodiscard]] bool field(ulint n, const byte *&data, ulint &len, bool &ext) const noexcept {
    if (n >= m_n_fields) {
      return false;
    }

    ulint start{};
    ulint end;
    bool null;

    ext = false;

    if (m_short) {
      start = n == 0 ? 0 : mach_read_from_1(m_rec - (REC_N_EXTRA_BYTES + n)) & ~REC_1BYTE_SQL_NULL_MASK;

      const auto info = mach_read_from_1(m_rec - (REC_N_EXTRA_BYTES + n + 1));

      end = info & ~REC_1BYTE_SQL_NULL_MASK;
      null = (info & REC_1BYTE_SQL_NULL_MASK) != 0;
    } else {
      constexpr ulint MASK = ~(REC_2BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK);

      start = n == 0 ? 0 : mach_read_from_2(m_rec - (REC_N_EXTRA_BYTES + 2 * n)) & MASK;

      const auto info = mach_read_from_2(m_rec - (REC_N_EXTRA_BYTES + 2 * n + 2));

      end = info & MASK;
      null = (info & REC_2BYTE_SQL_NULL_MASK) != 0;
      ext = (info & REC_2BYTE_EXTERN_MASK) != 0;
    }

    if (end < start || end > m_room) {
      return false;
    }

    data = m_rec + start;
    len = null ? UNIV_SQL_NULL : end - start;

    return true;
  }

  /** @return where the data of the record ends, from its origin. To be called after open(). */
  [[nodiscard]] ulint data_end() const noexcept {
    const auto n = m_n_fields - 1;

    if (m_short) {
      return mach_read_from_1(m_rec - (REC_N_EXTRA_BYTES + n + 1)) & ~REC_1BYTE_SQL_NULL_MASK;
    }

    return mach_read_from_2(m_rec - (REC_N_EXTRA_BYTES + 2 * n + 2)) & ~(REC_2BYTE_SQL_NULL_MASK | REC_2BYTE_EXTERN_MASK);
  }

  /** @return the bytes in front of the origin. */
  [[nodiscard]] ulint extra() const noexcept { return REC_N_EXTRA_BYTES + m_n_fields * (m_short ? 1 : 2); }

  const byte *m_rec{};
  ulint m_room{};
  ulint m_n_fields{};
  bool m_short{};
};

/** Compare key with the record at offs, the same as cmp_dtuple_rec().
@return false if what's at offs can't be compared. */
[[nodiscard]] bool raw_cmp(const Index *index, const DTuple *key, const page_t *page, ulint offs, int &cmp) noexcept {
  RawRec rec;

  if (!rec.open(page, offs)) {
    return false;
  }

  const auto key_min = (dtuple_get_info_bits(key) & REC_INFO_MIN_REC_FLAG) != 0;

  if ((mach_read_from_1(rec.m_rec - REC_INFO_BITS) & REC_INFO_MIN_REC_FLAG) != 0) {
    cmp = key_min ? 0 : 1;
    return true;
  } else if (key_min) {
    cmp = -1;
    return true;
  }

  const auto n = dtuple_get_n_fields_cmp(key);

  for (ulint i{}; i < n; ++i) {
    const byte *data{};
    ulint len{};
    bool ext{};

    if (!rec.field(i, data, len, ext)) {
      return false;
    }

    if (ext) {
      cmp = 0;
      return true;
    }

    const auto dfield = dtuple_get_nth_field(key, i);
    const auto type = dfield_get_type(dfield);

    /* These read the size of the type from the record, whatever it says it has. */
    if (len != UNIV_SQL_NULL && ((type->mtype == DATA_DOUBLE && len != sizeof(double)) || (type->mtype == DATA_FLOAT && len != sizeof(float)) || (type->mtype == DATA_DECIMAL && len == 0))) {
      return false;
    }

    cmp = cmp_data_data(
      index->m_cmp_ctx, type->mtype, type->prtype, static_cast<const byte *>(dfield_get_data(dfield)), dfield_get_len(dfield), data, len
    );

    if (cmp != 0) {
      return true;
    }
  }

  cmp = 0;

  return true;
}

#if defined(UNIV_DEBUG) || defined(UNIT_TESTING)
/** read_node_latched(), and what the version of the block was while it was owned: odd. */
[[nodiscard]] NodeStep read_node_at(
  System &sys, const Index *index, page_no_t page_no, const DTuple *key, ulint mode, Edge edge, ulint stop_level, uint64_t &version
) noexcept;
#endif /* UNIV_DEBUG || UNIT_TESTING */

[[nodiscard]] inline ulint raw_slot(const page_t *page, ulint n) noexcept {
  return mach_read_from_2(page + UNIV_PAGE_SIZE - PAGE_DIR - (n + 1) * PAGE_DIR_SLOT_SIZE);
}


}  // namespace

namespace {

/** @return false if offs can't be the origin of a record of the page. */
[[nodiscard]] inline bool raw_in_page(ulint offs) noexcept {
  return offs >= PAGE_DATA + REC_N_EXTRA_BYTES && offs < UNIV_PAGE_SIZE - PAGE_DIR;
}

/** @return the child in the node pointer at offs, false if it can't be one. */
[[nodiscard]] bool raw_child(const page_t *page, ulint offs, page_no_t &child) noexcept {
  RawRec rec;
  const byte *data{};
  ulint len{};
  bool ext{};

  if (!rec.open(page, offs) || !rec.field(rec.m_n_fields - 1, data, len, ext) || len != 4) {
    return false;
  }

  child = mach_read_from_4(data);

  return true;
}

/** Find the last record that's before the place of key, the infimum if none is, in a non-leaf mode.
@return false if the page can't be what it should be. */
[[nodiscard]] bool raw_search(const Index *index, const DTuple *key, const page_t *page, ulint mode, ulint &cur) noexcept {
  const auto n_slots = mach_read_from_2(page + PAGE_HEADER + PAGE_N_DIR_SLOTS);

  if (n_slots < 2 || n_slots > (UNIV_PAGE_SIZE - PAGE_DIR - PAGE_DATA) / PAGE_DIR_SLOT_SIZE) {
    return false;
  }

  /* @return true if a record that compares as c to the key is before the place of the key. */
  const auto is_before = [mode](int c) noexcept { return mode == PAGE_CUR_LE ? c >= 0 : c > 0; };

  int cmp{};
  ulint low{};
  ulint up = n_slots - 1;

  /* Slot 0 has the infimum, before anything, and the last the supremum, after anything. */
  while (up - low > 1) {
    const auto mid = (low + up) / 2;

    if (!raw_cmp(index, key, page, raw_slot(page, mid), cmp)) {
      return false;
    }

    if (is_before(cmp)) {
      low = mid;
    } else {
      up = mid;
    }
  }

  const auto end = raw_slot(page, up);

  cur = raw_slot(page, low);

  /* A slot owns at most this many records: a list that goes on is not a list of a page. */
  for (ulint n{};; ++n) {
    if (n > PAGE_DIR_SLOT_MAX_N_OWNED + 1 || !raw_in_page(cur)) {
      return false;
    }

    const auto next = mach_read_from_2(page + cur - REC_NEXT);

    if (next == end) {
      return true;
    }

    if (!raw_cmp(index, key, page, next, cmp)) {
      return false;
    }

    if (!is_before(cmp)) {
      return true;
    }

    cur = next;
  }
}

/** Set past to what past_high_key() tells.
@return false if the page can't be what it should be. */
[[nodiscard]] bool raw_past_high_key(const Index *index, const DTuple *key, const page_t *page, ulint mode, bool &past) noexcept {
  past = false;

  if (!page_has_right(page)) {
    return true;
  }

  const auto high_key = mach_read_from_2(page + PAGE_HEADER + PAGE_HIGH_KEY);

  if (high_key == 0) {
    return true;
  }

  int cmp{};

  if (!raw_cmp(index, key, page, high_key, cmp)) {
    return false;
  }

  past = mode == PAGE_CUR_GE || mode == PAGE_CUR_L ? cmp > 0 : cmp >= 0;

  return true;
}


}  // namespace

bool read_node_raw(
  const Index *index, const page_t *page, page_no_t page_no, const DTuple *key, ulint mode, Edge edge, ulint stop_level, NodeStep &step
) noexcept {
  /* Nothing from an attempt before this one. */
  step = NodeStep{};

  step.m_level = mach_read_from_2(page + PAGE_HEADER + PAGE_LEVEL);

  if (is_dead(page, index)) {
    step.m_kind = NodeStep::RESTART;
    return true;
  }

  if (step.m_level <= stop_level) {
    step.m_kind = NodeStep::STOP;
    step.m_page = page_no;
    return true;
  }

  const auto next = mach_read_from_4(page + FIL_PAGE_NEXT);
  ulint cur{PAGE_INFIMUM};

  switch (edge) {
    case Edge::KEY: {
      bool past;

      if (!raw_past_high_key(index, key, page, mode, past)) {
        return false;
      }

      if (past) {
        step.m_kind = NodeStep::RIGHT;
        step.m_page = next;
        return true;
      }

      if (!raw_search(index, key, page, node_mode(mode), cur)) {
        return false;
      }

      break;
    }

    case Edge::FIRST:
      cur = mach_read_from_2(page + PAGE_INFIMUM - REC_NEXT);
      break;

    case Edge::LAST: {
      if (page_has_right(page)) {
        step.m_kind = NodeStep::RIGHT;
        step.m_page = next;
        return true;
      }

      const auto n_slots = mach_read_from_2(page + PAGE_HEADER + PAGE_N_DIR_SLOTS);

      if (n_slots < 2 || n_slots > (UNIV_PAGE_SIZE - PAGE_DIR - PAGE_DATA) / PAGE_DIR_SLOT_SIZE) {
        return false;
      }

      const auto end = raw_slot(page, n_slots - 1);

      cur = raw_slot(page, n_slots - 2);

      for (ulint n{};; ++n) {
        if (n > PAGE_DIR_SLOT_MAX_N_OWNED + 1 || !raw_in_page(cur)) {
          return false;
        }

        const auto after = mach_read_from_2(page + cur - REC_NEXT);

        if (after == end) {
          break;
        }

        cur = after;
      }

      break;
    }

    case Edge::RANDOM: {
      const auto n_recs = mach_read_from_2(page + PAGE_HEADER + PAGE_N_RECS);

      if (n_recs == 0) {
        return false;
      }

      cur = mach_read_from_2(page + PAGE_INFIMUM - REC_NEXT);

      for (auto n = ulint(PageCursor::lcg_prng() % n_recs); n > 0; --n) {
        if (!raw_in_page(cur)) {
          return false;
        }

        cur = mach_read_from_2(page + cur - REC_NEXT);
      }

      break;
    }
  }

  /* Deletes don't reach non-leaf pages, and a key that got here is >= the first node pointer. */
  if (cur == PAGE_INFIMUM || cur == PAGE_SUPREMUM || !raw_child(page, cur, step.m_page)) {
    return false;
  }

  step.m_kind = NodeStep::CHILD;

  return true;
}

bool read_family_raw(const Index *index, const page_t *page, const DTuple *key, ulint level, RawFamily &fam) noexcept {
  fam = RawFamily{};

  if (is_dead(page, index) || mach_read_from_2(page + PAGE_HEADER + PAGE_LEVEL) != level) {
    fam.m_kind = is_dead(page, index) ? RawFamily::DEAD : RawFamily::OTHER_LEVEL;
    return true;
  }

  bool past;

  if (!raw_past_high_key(index, key, page, PAGE_CUR_LE, past)) {
    return false;
  }

  if (past) {
    fam.m_kind = RawFamily::RIGHT;
    fam.m_next = mach_read_from_4(page + FIL_PAGE_NEXT);
    return true;
  }

  ulint cur{};

  if (!raw_search(index, key, page, PAGE_CUR_LE, cur)) {
    return false;
  }

  /* The first of the list, if the key is before it. */
  if (cur == PAGE_INFIMUM) {
    cur = mach_read_from_2(page + PAGE_INFIMUM - REC_NEXT);
  }

  /* Before and after it by the list, from the start of it: a merge is rare, a page is short. */
  ulint prev{PAGE_INFIMUM};
  const auto n_recs = mach_read_from_2(page + PAGE_HEADER + PAGE_N_RECS);
  ulint at = mach_read_from_2(page + PAGE_INFIMUM - REC_NEXT);

  for (ulint n{}; at != cur; ++n) {
    if (n > n_recs || !raw_in_page(at)) {
      return false;
    }

    prev = at;
    at = mach_read_from_2(page + at - REC_NEXT);
  }

  if (!raw_in_page(cur) || cur == PAGE_SUPREMUM) {
    return false;
  }

  const auto next = mach_read_from_2(page + cur - REC_NEXT);

  fam.m_kind = RawFamily::FOUND;
  fam.m_n_children = n_recs;

  if (!raw_child(page, cur, fam.m_page)) {
    return false;
  }

  if (prev != PAGE_INFIMUM && !raw_child(page, prev, fam.m_left)) {
    return false;
  }

  if (next != PAGE_SUPREMUM && !raw_child(page, next, fam.m_right)) {
    return false;
  }

  return true;
}

bool read_leaf_raw(const Index *index, const page_t *page, const DTuple *key, ulint mode, Edge edge, RawLeaf &leaf) noexcept {
  leaf = RawLeaf{};

  if (is_dead(page, index)) {
    leaf.m_kind = RawLeaf::DEAD;
    return true;
  }

  if (mach_read_from_2(page + PAGE_HEADER + PAGE_LEVEL) != 0) {
    leaf.m_kind = RawLeaf::NOT_LEAF;
    return true;
  }

  const auto next = mach_read_from_4(page + FIL_PAGE_NEXT);

  switch (edge) {
    case Edge::FIRST:
      leaf.m_offs = PAGE_INFIMUM;
      break;

    case Edge::LAST:
      if (page_has_right(page)) {
        leaf.m_kind = RawLeaf::RIGHT;
        leaf.m_next = next;
        return true;
      }

      leaf.m_offs = PAGE_SUPREMUM;
      break;

    case Edge::KEY: {
      bool past;

      if (!raw_past_high_key(index, key, page, mode, past)) {
        return false;
      }

      if (past) {
        leaf.m_kind = RawLeaf::RIGHT;
        leaf.m_next = next;
        return true;
      }

      ulint cur{};

      if (!raw_search(index, key, page, node_mode(mode), cur)) {
        return false;
      }

      /* That's the last record before the place of the key. These want what's after it. */
      if (mode == PAGE_CUR_G || mode == PAGE_CUR_GE) {
        if (!raw_in_page(cur)) {
          return false;
        }

        cur = mach_read_from_2(page + cur - REC_NEXT);
      }

      leaf.m_offs = cur;
      break;
    }

    case Edge::RANDOM:
      return false;
  }

  leaf.m_kind = RawLeaf::FOUND;

  return true;
}

bool read_group_raw(const page_t *page, ulint offs, ulint &bit) noexcept {
  auto owner = offs;

  /* The records of a group go up to their owner, which has the count. */
  for (ulint n{}; n <= PAGE_DIR_SLOT_MAX_N_OWNED + 1; ++n) {
    if (!raw_in_page(owner)) {
      return false;
    }

    if ((mach_read_from_1(page + owner - REC_N_OWNED) & REC_N_OWNED_MASK) != 0) {
      bit = BufBlock::group_bit(owner);
      return true;
    }

    owner = mach_read_from_2(page + owner - REC_NEXT);
  }

  return false;
}

bool read_prev_raw(const page_t *page, ulint offs, ulint &prev) noexcept {
  const auto n_recs = mach_read_from_2(page + PAGE_HEADER + PAGE_N_RECS);

  prev = PAGE_INFIMUM;

  for (ulint n{}; n <= ulint(n_recs) + 1; ++n) {
    if (!raw_in_page(prev)) {
      return false;
    }

    const auto next = mach_read_from_2(page + prev - REC_NEXT);

    if (next == offs) {
      return true;
    }

    prev = next;
  }

  return false;
}

bool read_rec_copy_raw(const page_t *page, ulint offs, byte *to) noexcept {
  memcpy(to, page, PAGE_SUPREMUM_END);

  if (offs == PAGE_INFIMUM || offs == PAGE_SUPREMUM) {
    return true;
  }

  RawRec rec;

  if (!rec.open(page, offs)) {
    return false;
  }

  const auto extra = rec.extra();
  const auto end = offs + rec.data_end();

  if (end > UNIV_PAGE_SIZE - PAGE_DIR) {
    return false;
  }

  memcpy(to + offs - extra, page + offs - extra, extra + end - offs);

  return true;
}

bool read_rec_size_raw(const Index *index, const page_t *page, ulint level, ulint &size) noexcept {
  size = ULINT_UNDEFINED;

  if (is_dead(page, index) || mach_read_from_2(page + PAGE_HEADER + PAGE_LEVEL) != level) {
    return true;
  }

  /* What page_get_rec_data_size() tells. */
  auto data = ulint(mach_read_from_2(page + PAGE_HEADER + PAGE_HEAP_TOP)) - PAGE_SUPREMUM_END - mach_read_from_2(page + PAGE_HEADER + PAGE_GARBAGE);

  if (page_has_right(page)) {
    if (const auto high_key = mach_read_from_2(page + PAGE_HEADER + PAGE_HIGH_KEY); high_key != 0) {
      RawRec rec;

      if (!rec.open(page, high_key)) {
        return false;
      }

      data -= rec.extra() + rec.data_end();
    }
  }

  size = data;

  return true;
}

NodeStep read_node(
  System &sys, const Index *index, page_no_t page_no, const DTuple *key, ulint mode, Edge edge, ulint stop_level
) noexcept {
  Mtr mtr;

  mtr.start();

  const auto block = sys.m_btree->block_get(index->get_space_id(), page_no, RW_NO_LATCH, &mtr);

  NodeStep step{};

  [[maybe_unused]] const auto version =
    read_stable(sys, block, [&]() noexcept { return read_node_raw(index, block->get_frame(), page_no, key, mode, edge, stop_level, step); });

  mtr.commit(sys);

#ifdef UNIV_DEBUG
  /* Unless the page has changed since, the search under the latch has to agree. So every test
  checks this search, on the types of keys that test has, and PageCursor's is the definition of
  right. Not for a random one. */
  if (edge != Edge::RANDOM) {
    uint64_t seen{};
    const auto latched = read_node_at(sys, index, page_no, key, mode, edge, stop_level, seen);

    if (seen == version + 1 && (latched.m_kind != step.m_kind || latched.m_page != step.m_page || latched.m_level != step.m_level)) {
      log_err(std::format(
        "A read of page {} with no latch and with it differ: kind {} and {}, page {} and {}, level {} and {}, mode {}, edge {}, stop {}",
        page_no, int(step.m_kind), int(latched.m_kind), step.m_page, latched.m_page, step.m_level, latched.m_level, mode, int(edge), stop_level
      ));

      if (key != nullptr) {
        dtuple_print(stderr, key);
      }

      ut_error;
    }
  }
#endif /* UNIV_DEBUG */

  return step;
}

#if defined(UNIV_DEBUG) || defined(UNIT_TESTING)
NodeStep read_node_latched(
  System &sys, const Index *index, page_no_t page_no, const DTuple *key, ulint mode, Edge edge, ulint stop_level
) noexcept {
  uint64_t version{};

  return read_node_at(sys, index, page_no, key, mode, edge, stop_level, version);
}

namespace {

NodeStep read_node_at(
  System &sys, const Index *index, page_no_t page_no, const DTuple *key, ulint mode, Edge edge, ulint stop_level, uint64_t &version
) noexcept {
  Mtr mtr;

  mtr.start();

  auto block = sys.m_btree->block_get(index->get_space_id(), page_no, RW_B_LATCH, &mtr);
  auto page = block->get_frame();

  version = block->read_begin();

  NodeStep step{};

  step.m_level = sys.m_btree->page_get_level(page, &mtr);

  if (is_dead(page, index)) {

    step.m_kind = NodeStep::RESTART;

  } else if (step.m_level <= stop_level) {

    step.m_kind = NodeStep::STOP;
    step.m_page = page_no;

  } else if (edge == Edge::KEY ? past_high_key(index, page, key, mode) : edge == Edge::LAST && page_has_right(page)) {

    step.m_kind = NodeStep::RIGHT;
    step.m_page = Btree::page_get_next(page, &mtr);

  } else {
    PageCursor cur{};

    switch (edge) {
      case Edge::KEY:
        cur.search(sys, block, index, key, node_mode(mode));
        break;
      case Edge::FIRST:
        cur.set_before_first(block);
        cur.move_to_next();
        break;
      case Edge::LAST:
        cur.set_after_last(block);
        cur.move_to_prev();
        break;
      case Edge::RANDOM:
        cur.open_on_rnd_user_rec(sys, block);
        break;
    }

    const auto node_ptr = cur.get_rec();

    /* Deletes don't reach non-leaf pages, and a key that got here is >= the first node pointer. */
    ut_a(page_rec_is_user_rec(node_ptr));

    std::array<ulint, REC_OFFS_NORMAL_SIZE> offs_{};
    MemHeap *heap{};

    rec_offs_init(offs_);

    const RecOffs offs = PhyRec(index, node_ptr).get_all_col_offsets(offs_.data(), &heap, Current_location());

    step.m_kind = NodeStep::CHILD;
    step.m_page = sys.m_btree->node_ptr_get_child_page_no(node_ptr, offs);

    if (heap != nullptr) {
      mem_heap_free(heap);
    }
  }

  mtr.commit(sys);

  return step;
}

}  // namespace
#endif /* UNIV_DEBUG || UNIT_TESTING */

}  // namespace blink
