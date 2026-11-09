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

/** @file idx/idx0mem.cc
An index in memory, for tests: the proof that the engine above IndexAm needs nothing of a tree. The
records of an index are a sorted array, in the record format of rem0rec.h, nothing is on a page,
nothing is logged and nothing survives a restart.

What it is not: concurrent. A call takes the index's mutex and leaves it, a cursor holds nothing
between calls (its record stays readable: memory of a record is freed only with the index), so a
reader and a writer can interleave in ways a real index would forbid. It's for the row layer's tests
that run one thread at a time.

Positions: a record, the start (before the first record) and the fence at the end, which bounds the
gap after the last record for the locks. There are no other fences: an insert's boundary after it is
the next record or the end. */

#include "idx0am.h"

#include "btr0types.h"
#include "data0data.h"
#include "dict0dict.h"
#include "lock0lock.h"
#include "mem0mem.h"
#include "rem0cmp.h"
#include "rem0rec.h"
#include "row0row.h"
#include "row0upd.h"
#include "srv0sys.h"
#include "trx0undo_rec.h"
#include "trx0trx.h"

#include <algorithm>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace {

/** Records to a "page": for what is counted in pages, statistics and the parts of a range. */
constexpr size_t RECS_PER_PAGE = 64;

/** The records of one index. */
struct MemTree {
  std::mutex m_mutex;

  /** In key order. */
  std::vector<Rec> m_recs;

  /** Where the records are: freed with the tree, a cursor's record stays readable. */
  std::vector<std::unique_ptr<byte[]>> m_memory;

  /** What a position that isn't a record stands on, for the code that reads a record there. */
  Rec m_start{};
  Rec m_end{};

  /** A record of the tree from entry. */
  [[nodiscard]] Rec make(const Index *index, const DTuple *entry, ulint n_ext) {
    const auto size = rec_get_converted_size(index, entry, n_ext);
    auto &buf = m_memory.emplace_back(std::make_unique<byte[]>(size + 8));

    return Rec::convert_dtuple_to_rec(buf.get(), index, entry, n_ext);
  }

  /** The records that stand for the start and the end, as a page has them: what reads them sees a system
  record by its status. */
  void make_edges(const Index *index) {
    if (!m_start.is_null()) {
      return;
    }

    auto heap = mem_heap_create(256);

    for (auto [status, data, at] : {std::tuple{REC_STATUS_INFIMUM, "I", &m_start}, std::tuple{REC_STATUS_SUPREMUM, "S", &m_end}}) {
      auto t = dtuple_create(heap, 1);
      auto f = dtuple_get_nth_field(t, 0);

      dtuple_set_info_bits(t, status);
      dfield_set_data(f, data, 1);
      dtype_set(dfield_get_type(f), DATA_VARCHAR, DATA_ENGLISH | DATA_NOT_NULL, 1);

      *at = make(index, t, 0);
    }

    mem_heap_free(heap);
  }
};

/** The trees, by index id: the root of an index is its id. */
struct MemAm final : IndexAm {
  explicit MemAm(System &sys) noexcept : m_sys(sys) {}

  [[nodiscard]] MemTree &tree(const Index *index) noexcept {
    std::lock_guard lock{m_mutex};

    auto &t = m_trees[index->m_id];

    if (t == nullptr) {
      /* An index of the catalog whose records went with a restart: empty. */
      t = std::make_unique<MemTree>();
    }

    t->make_edges(index);

    return *t;
  }

  std::unique_ptr<Cursor> cursor() override;

  Root create(ulint, space_id_t, uint64_t index_id, Index *, Mtr *) noexcept override {
    std::lock_guard lock{m_mutex};

    m_trees[index_id] = std::make_unique<MemTree>();

    return Root{index_id};
  }

  void attach(Index *, Root) noexcept override {}

  void drop_data(space_id_t, Root) noexcept override {}

  void drop_root(space_id_t, Root root, Mtr *) noexcept override {
    std::lock_guard lock{m_mutex};

    m_trees.erase(root);
  }

  [[nodiscard]] ulint n_recs(const Index *index) noexcept {
    auto &t = tree(index);
    std::lock_guard lock{t.m_mutex};

    return t.m_recs.size();
  }

  /* As many "pages" as a tree of small pages would have: what the statistics want. */
  ulint n_pages(const Index *index) noexcept override { return 1 + n_recs(index) / RECS_PER_PAGE; }

  ulint n_data_pages(const Index *index) noexcept override { return n_pages(index); }

  void estimate_distinct(const Index *index) noexcept override;

  void print(Index *index) noexcept override { log_info(std::format("in memory index {}: {} records", index->m_name, n_recs(index))); }

  bool validate(Index *index, Trx *) noexcept override;

  std::vector<const DTuple *> split_range(const Index *index, const DTuple *start, const DTuple *end, size_t n, MemHeap *heap) noexcept override;

  System &m_sys;
  std::mutex m_mutex;
  std::map<uint64_t, std::unique_ptr<MemTree>> m_trees;
};

/** The offsets of rec, in heap. */
[[nodiscard]] RecOffs offsets_of(const Index *index, Rec rec, MemHeap *&heap) {
  return PhyRec(index, rec).get_all_col_offsets(nullptr, &heap, Current_location());
}

/** @return cmp of key with rec, and the fields they share. */
[[nodiscard]] int compare(const Index *index, const DTuple *key, Rec rec, ulint &matched) {
  MemHeap *heap{};
  const auto offs = offsets_of(index, rec, heap);
  ulint bytes{};

  matched = 0;

  const auto cmp = cmp_dtuple_rec_with_match(index->m_cmp_ctx, key, rec, offs, &matched, &bytes);

  mem_heap_free(heap);

  return cmp;
}

/** The key of rec, a copy in heap. */
[[nodiscard]] DTuple *key_copy(const Index *index, Rec rec, MemHeap *heap) {
  ulint n_fields;
  byte *buf{};
  ulint buf_size{};
  const auto prefix = index->copy_rec_order_prefix(rec, &n_fields, buf, buf_size);
  const auto bytes = static_cast<byte *>(mem_heap_alloc(heap, buf_size));

  memcpy(bytes, buf, buf_size);
  mem_free(buf);

  return index->build_data_tuple(Rec(bytes + (prefix.get() - buf)), n_fields, heap);
}

struct MemCursor final : IndexAm::Cursor {
  explicit MemCursor(System &sys, MemAm &am) noexcept : m_sys(sys), m_am(am) {}

  ~MemCursor() noexcept override {
    if (m_heap != nullptr) {
      mem_heap_free(m_heap);
    }
  }

  /** Where in the tree: m_at says which, m_pos is the record's place for At::REC. */
  void place(const Index *index, size_t pos, At at) noexcept {
    m_index = index;
    m_pos = pos;
    m_at = at;
  }

  /** The first record with key cmp > 0 (or >= 0 with ge): its place, the size if none. */
  [[nodiscard]] size_t bound(MemTree &t, const DTuple *key, bool ge) noexcept {
    size_t low{};
    size_t up{t.m_recs.size()};

    while (low < up) {
      const auto mid = (low + up) / 2;
      ulint matched;
      const auto cmp = compare(m_index, key, t.m_recs[mid], matched);

      /* key < rec: rec is past it. key == rec: past it for G, not for GE. */
      if (cmp < 0 || (ge && cmp == 0)) {
        up = mid;
      } else {
        low = mid + 1;
      }
    }

    return low;
  }

  /** Search: the position mode says, and how much the key shares with what's around it. */
  void search(const Index *index, const DTuple *key, SearchMode mode) noexcept {
    auto &t = m_am.tree(index);
    std::lock_guard lock{t.m_mutex};

    m_index = index;
    m_mode = mode;

    switch (mode) {
      case PAGE_CUR_GE:
      case PAGE_CUR_G: {
        const auto at = bound(t, key, mode == PAGE_CUR_GE);

        place(index, at, at < t.m_recs.size() ? At::REC : At::FENCE);
        break;
      }
      default: {
        /* L and LE: the last record < or <= key. */
        const auto at = bound(t, key, mode == PAGE_CUR_L);

        if (at == 0) {
          place(index, 0, At::START);
        } else {
          place(index, at - 1, At::REC);
        }
      }
    }

    /* The matches with the records on both sides of the position. */
    m_low_match = 0;
    m_up_match = 0;

    const auto before = m_at == At::REC ? (mode == PAGE_CUR_GE || mode == PAGE_CUR_G ? ptrdiff_t(m_pos) - 1 : ptrdiff_t(m_pos))
                                        : (m_at == At::FENCE ? ptrdiff_t(t.m_recs.size()) - 1 : ptrdiff_t(-1));
    const auto after = before + 1;

    if (before >= 0) {
      (void)compare(index, key, t.m_recs[size_t(before)], m_low_match);
    }

    if (after < ptrdiff_t(t.m_recs.size())) {
      (void)compare(index, key, t.m_recs[size_t(after)], m_up_match);
    }
  }

  void seek(Index *index, const DTuple *key, SearchMode mode, Intent, Mtr *, SourceLocation) noexcept override { search(index, key, mode); }

  void position(const Index *index, const DTuple *key, SearchMode mode, Intent, Mtr *, SourceLocation) noexcept override {
    search(index, key, mode);
  }

  void seek_insert(const Index *index, const DTuple *entry, Intent, QueThr *, Mtr *, SourceLocation) noexcept override {
    search(index, entry, PAGE_CUR_LE);
  }

  bool find(const Index *index, const DTuple *key, Intent, Mtr *, SourceLocation) noexcept override {
    search(index, key, PAGE_CUR_LE);

    return m_at == At::REC && m_low_match >= dtuple_get_n_fields_cmp(key);
  }

  void seek_edge(const Index *index, bool first, Intent, Mtr *, SourceLocation) noexcept override {
    auto &t = m_am.tree(index);
    std::lock_guard lock{t.m_mutex};

    place(index, 0, first ? At::START : At::FENCE);
  }

  [[nodiscard]] bool step(bool forward, bool positions) noexcept {
    auto &t = m_am.tree(m_index);
    std::lock_guard lock{t.m_mutex};
    const auto n = t.m_recs.size();

    /* The records may have changed since: a position stays a place in the array, for one thread. */
    if (m_at == At::REC && m_pos >= n) {
      place(m_index, n, At::FENCE);
    }

    if (forward) {
      switch (m_at) {
        case At::START:
          if (n > 0) {
            place(m_index, 0, At::REC);
            return true;
          }
          place(m_index, 0, At::FENCE);
          return positions;
        case At::REC:
          if (m_pos + 1 < n) {
            place(m_index, m_pos + 1, At::REC);
            return true;
          }
          place(m_index, n, At::FENCE);
          return positions;
        case At::FENCE:
          return false;
      }
    } else {
      switch (m_at) {
        case At::FENCE:
          if (n > 0) {
            place(m_index, n - 1, At::REC);
            return true;
          }
          place(m_index, 0, At::START);
          return positions;
        case At::REC:
          if (m_pos > 0) {
            place(m_index, m_pos - 1, At::REC);
            return true;
          }
          place(m_index, 0, At::START);
          return positions;
        case At::START:
          return false;
      }
    }

    return false;
  }

  bool next(Mtr *) noexcept override { return step(true, false); }

  bool prev(Mtr *) noexcept override { return step(false, false); }

  bool next_pos(Mtr *) noexcept override { return step(true, true); }

  bool prev_pos(Mtr *) noexcept override { return step(false, true); }

  bool on_rec() const noexcept override { return m_at == At::REC; }

  Rec rec() noexcept override {
    auto &t = m_am.tree(m_index);
    std::lock_guard lock{t.m_mutex};

    switch (m_at) {
      case At::REC:
        return m_pos < t.m_recs.size() ? t.m_recs[m_pos] : t.m_end;
      case At::START:
        return t.m_start;
      case At::FENCE:
        return t.m_end;
    }

    return t.m_end;
  }

  const Index *index() const noexcept override { return m_index; }

  ulint low_match() noexcept override { return m_low_match; }

  ulint up_match() noexcept override { return m_up_match; }

  SearchMode search_mode() const noexcept override { return m_mode; }

#ifdef UNIT_TESTING
  BufBlock *block() noexcept override { return nullptr; }
#endif /* UNIT_TESTING */

  LockPos lock_pos(Rec rec) noexcept override {
    auto &t = m_am.tree(m_index);

    if (rec == t.m_end) {
      return LockPos{lock::end_of(m_index), true};
    }

    MemHeap *heap{};
    const auto offs = offsets_of(m_index, rec, heap);
    const auto key = lock::key_of(m_index, rec, offs.get());

    mem_heap_free(heap);

    return LockPos{key, false};
  }

  At at() noexcept override { return m_at; }

  Next peek_next() noexcept override {
    auto &t = m_am.tree(m_index);
    std::lock_guard lock{t.m_mutex};
    const auto next = m_at == At::START ? size_t{0} : m_at == At::REC ? m_pos + 1 : t.m_recs.size();

    return next < t.m_recs.size() ? Next{t.m_recs[next], At::REC} : Next{t.m_end, At::FENCE};
  }

  /** The boundary after the position, for the locks of an insert there. */
  [[nodiscard]] LockKey next_boundary() noexcept {
    const auto next = peek_next();

    return next.m_at == At::FENCE ? lock::end_of(m_index) : lock_pos(next.m_rec).m_key;
  }

  void save(Mtr *) noexcept override {
    if (m_heap == nullptr) {
      m_heap = mem_heap_create(256);
    } else {
      mem_heap_empty(m_heap);
    }

    m_saved_index = m_index;

    switch (m_at) {
      case At::REC:
        m_saved_key = key_copy(m_index, rec(), m_heap);
        m_saved = SavedAt::On;
        break;
      case At::START:
        m_saved_key = nullptr;
        m_saved = SavedAt::Before;
        break;
      case At::FENCE:
        m_saved_key = nullptr;
        m_saved = SavedAt::After;
        break;
    }
  }

  bool restore(Intent intent, Mtr *mtr, SourceLocation loc) noexcept override {
    ut_a(m_saved != SavedAt::None);

    if (m_saved_key == nullptr) {
      seek_edge(m_saved_index, m_saved == SavedAt::Before, intent, mtr, loc);
      return false;
    }

    /* On it if it's there, else just before where it was: the next record is the next to read. */
    return find(m_saved_index, m_saved_key, intent, mtr, loc);
  }

  SavedAt saved_at() const noexcept override { return m_saved; }

  bool has_saved() const noexcept override { return m_saved != SavedAt::None; }

  void copy_saved(Cursor &src) noexcept override {
    auto &from = static_cast<MemCursor &>(src);

    if (m_heap == nullptr) {
      m_heap = mem_heap_create(256);
    }

    m_saved = from.m_saved;
    m_saved_index = from.m_saved_index;
    m_saved_key = from.m_saved_key == nullptr ? nullptr : dtuple_copy(from.m_saved_key, m_heap);

    if (m_saved_key != nullptr) {
      for (ulint i{}; i < dtuple_get_n_fields(m_saved_key); ++i) {
        dfield_dup(dtuple_get_nth_field(m_saved_key, i), m_heap);
      }
    }
  }

  void commit(Mtr *mtr) noexcept override { mtr->commit(m_sys); }

  DbErr remove(Intent, TrxRbCtx, Mtr *) noexcept override {
    ut_a(m_at == At::REC);

    auto &t = m_am.tree(m_index);
    const auto here = rec();
    const auto key = lock_pos(here).m_key;

    /* The boundary after it: the next record or the end. */
    const auto next = next_boundary();

    m_sys.m_lock_sys->update_delete(m_index, key, next);

    std::lock_guard lock{t.m_mutex};

    t.m_recs.erase(t.m_recs.begin() + ptrdiff_t(m_pos));

    /* Just before where it was, as a delete leaves a cursor. */
    if (m_pos == 0) {
      place(m_index, 0, At::START);
    } else {
      place(m_index, m_pos - 1, At::REC);
    }

    return DB_SUCCESS;
  }

  /** @return true if entry is more than one record of the format can hold: nothing is stored apart from a record. */
  [[nodiscard]] bool too_big(const DTuple *entry, ulint n_ext) const noexcept {
    return rec_get_converted_size(m_index, entry, n_ext) >= REC_MAX_DATA_SIZE;
  }

  /** The lock check and the undo of a change of the record of the cursor, as BtreeCursor does it. */
  [[nodiscard]] DbErr lock_and_undo(ulint flags, const Upd *update, ulint cmpl_info, QueThr *thr, roll_ptr_t *roll_ptr) noexcept {
    const auto here = rec();

    if (!m_index->is_clustered()) {
      return m_sys.m_lock_sys->sec_rec_modify_check_and_lock(flags, lock_pos(here).m_key, m_index, thr);
    }

    if (!(flags & BTR_NO_LOCKING_FLAG)) {
      MemHeap *heap{};
      const auto offs = offsets_of(m_index, here, heap);
      const auto err = m_sys.m_lock_sys->clust_rec_modify_check_and_lock(flags, lock::key_of(m_index, here, offs.get()), here, m_index, offs.get(), thr);

      mem_heap_free(heap);

      if (err != DB_SUCCESS) {
        return err;
      }
    }

    return TrxUndoRecord::report_row_operation(m_sys, flags, TRX_UNDO_MODIFY_OP, thr, m_index, nullptr, update, cmpl_info, here, roll_ptr);
  }

  DbErr update(Intent, ulint flags, MemHeap **heap, BigRec *big_rec, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *) noexcept override {
    ut_a(m_at == At::REC);

    /* Nothing is ever stored apart from a record. */
    if (big_rec != nullptr) {
      *big_rec = BigRec{};
    }

    /* The caller's heap if it has one, for the new version's fields, else one of the call's. */
    MemHeap *own{};

    if (heap == nullptr) {
      heap = &own;
    }

    if (*heap == nullptr) {
      *heap = mem_heap_create(1024);
    }

    /* The new version of the record, the same key: it takes the place of the old one. */
    const auto here = rec();
    MemHeap *offs_heap{};
    const auto offs = offsets_of(m_index, here, offs_heap);

    /* No field changes size: the record changes where it is, as on a page. */
    if (!m_sys.m_row_upd->changes_field_size_or_external(m_index, offs.get(), update)) {
      mem_heap_free(offs_heap);

      if (own != nullptr) {
        mem_heap_free(own);
      }

      return update_in_place(flags, update, cmpl_info, thr, nullptr);
    }

    ulint n_ext;
    auto entry = row_rec_to_index_entry(ROW_COPY_DATA, here, m_index, offs, &n_ext, *heap);

    mem_heap_free(offs_heap);

    m_sys.m_row_upd->index_replace_new_col_vals_index_pos(entry, m_index, update, false, *heap);

    /* Before anything changes: a record is all there is, it has to fit the format. */
    if (too_big(entry, n_ext)) {
      if (own != nullptr) {
        mem_heap_free(own);
      }

      return DB_TOO_BIG_RECORD;
    }

    roll_ptr_t roll_ptr{};

    if (const auto err = lock_and_undo(flags, update, cmpl_info, thr, &roll_ptr); err != DB_SUCCESS) {
      if (own != nullptr) {
        mem_heap_free(own);
      }

      return err;
    }

    if (m_index->is_clustered() && !(flags & BTR_KEEP_SYS_FLAG)) {
      m_sys.m_row_upd->index_entry_sys_field(entry, m_index, DATA_TRX_ID, thr_get_trx(thr)->m_id);
      m_sys.m_row_upd->index_entry_sys_field(entry, m_index, DATA_ROLL_PTR, roll_ptr);
    }

    auto &t = m_am.tree(m_index);
    std::lock_guard lock{t.m_mutex};

    t.m_recs[m_pos] = t.make(m_index, entry, n_ext);

    if (own != nullptr) {
      mem_heap_free(own);
    }

    return DB_SUCCESS;
  }

  DbErr update_in_place(ulint flags, const Upd *update, ulint cmpl_info, QueThr *thr, Mtr *) noexcept override {
    ut_a(m_at == At::REC);

    roll_ptr_t roll_ptr{};

    if (const auto err = lock_and_undo(flags, update, cmpl_info, thr, &roll_ptr); err != DB_SUCCESS) {
      return err;
    }

    const auto here = rec();
    MemHeap *heap{};
    const auto offs = offsets_of(m_index, here, heap);

    if (m_index->is_clustered() && !(flags & BTR_KEEP_SYS_FLAG)) {
      m_sys.m_row_upd->rec_sys_fields(here, m_index, offs.get(), thr_get_trx(thr), roll_ptr);
    }

    m_sys.m_row_upd->rec_in_place(here, m_index, offs.get(), update);

    mem_heap_free(heap);

    return DB_SUCCESS;
  }

  DbErr insert(Intent, ulint flags, DTuple *entry, BigRec *big_rec, ulint n_ext, QueThr *thr, Mtr *) noexcept override {
    if (big_rec != nullptr) {
      *big_rec = BigRec{};
    }

    if (too_big(entry, n_ext)) {
      return DB_TOO_BIG_RECORD;
    }

    /* The boundary after the place of the record: its gap is what the insert goes into. */
    const auto next = next_boundary();
    bool inherit{};

    if (const auto err = m_sys.m_lock_sys->rec_insert_check_and_lock(flags, next, m_index, thr, &inherit); err != DB_SUCCESS) {
      return err;
    }

    if (m_index->is_clustered()) {
      roll_ptr_t roll_ptr;

      const auto err = TrxUndoRecord::report_row_operation(m_sys, flags, TRX_UNDO_INSERT_OP, thr, m_index, entry, nullptr, 0, nullptr, &roll_ptr);

      if (err != DB_SUCCESS) {
        return err;
      }

      if (!(flags & BTR_KEEP_SYS_FLAG)) {
        m_sys.m_row_upd->index_entry_sys_field(entry, m_index, DATA_ROLL_PTR, roll_ptr);
      }
    }

    auto &t = m_am.tree(m_index);
    Rec rec;

    {
      std::lock_guard lock{t.m_mutex};

      const auto at = m_at == At::START ? size_t{0} : m_at == At::REC ? m_pos + 1 : t.m_recs.size();

      rec = t.make(m_index, entry, n_ext);
      t.m_recs.insert(t.m_recs.begin() + ptrdiff_t(at), rec);

      place(m_index, at, At::REC);
    }

    if (!(flags & BTR_NO_LOCKING_FLAG) && inherit) {
      m_sys.m_lock_sys->update_insert(m_index, lock_pos(rec).m_key, next);
    }

    return DB_SUCCESS;
  }

  DbErr store_big_rec(const RecOffs &, const BigRec &big_rec, Mtr *) noexcept override {
    /* A record of any size is a record here: nothing is stored apart from it. */
    return big_rec.n_fields == 0 ? DB_SUCCESS : DB_UNSUPPORTED;
  }

  DbErr set_delete_mark(ulint flags, bool val, QueThr *thr, Mtr *) noexcept override {
    ut_a(m_at == At::REC);

    auto here = rec();

    if (m_index->is_clustered()) {
      roll_ptr_t roll_ptr{};

      if (const auto err = lock_and_undo(flags, nullptr, 0, thr, &roll_ptr); err != DB_SUCCESS) {
        return err;
      }

      MemHeap *heap{};
      const auto offs = offsets_of(m_index, here, heap);

      here.set_deleted_flag(val);

      if (!(flags & BTR_KEEP_SYS_FLAG)) {
        m_sys.m_row_upd->rec_sys_fields(here, m_index, offs.get(), thr_get_trx(thr), roll_ptr);
      }

      mem_heap_free(heap);

      return DB_SUCCESS;
    }

    if (const auto err = m_sys.m_lock_sys->sec_rec_modify_check_and_lock(flags, lock_pos(here).m_key, m_index, thr); err != DB_SUCCESS) {
      return err;
    }

    /* The mark and the last modifier change together. */
    here.set_deleted_flag(val);
    mach_write_to_8(const_cast<byte *>(here.get()) + row_get_sec_trx_id_offset(here), thr_get_trx(thr)->m_id);

    return DB_SUCCESS;
  }

  void close() noexcept override { m_saved = SavedAt::None; }

  System &m_sys;
  MemAm &m_am;
  const Index *m_index{};
  size_t m_pos{};
  At m_at{At::START};
  SearchMode m_mode{PAGE_CUR_GE};
  ulint m_low_match{};
  ulint m_up_match{};

  SavedAt m_saved{SavedAt::None};
  const Index *m_saved_index{};
  DTuple *m_saved_key{};
  MemHeap *m_heap{};
};

std::unique_ptr<IndexAm::Cursor> MemAm::cursor() { return std::make_unique<MemCursor>(m_sys, *this); }

bool MemAm::validate(Index *index, Trx *) noexcept {
  auto &t = tree(index);
  std::lock_guard lock{t.m_mutex};

  for (size_t i{1}; i < t.m_recs.size(); ++i) {
    MemHeap *heap{};
    const auto key = key_copy(index, t.m_recs[i - 1], heap = mem_heap_create(256));
    ulint matched;
    const auto cmp = compare(index, key, t.m_recs[i], matched);

    mem_heap_free(heap);

    if (cmp >= 0) {
      log_err(std::format("in memory index {}: records {} and {} out of order", index->m_name, i - 1, i));
      return false;
    }
  }

  return true;
}

void MemAm::estimate_distinct(const Index *index) noexcept {
  auto i = const_cast<Index *>(index);
  const auto n_uniq = index->get_n_unique();

  i->m_stats.m_index_size = page_no_t(n_pages(index));
  i->m_stats.m_n_leaf_pages = page_no_t(n_pages(index));

  /* Exact, it's all here: [j] is the number of distinct prefixes of j fields, a prefix is new where it
  differs from the record before it. */
  auto &t = tree(index);
  std::lock_guard lock{t.m_mutex};
  auto heap = mem_heap_create(256);

  for (ulint j{}; j <= n_uniq; ++j) {
    i->m_stats.m_n_diff_key_vals[j] = t.m_recs.empty() ? 0 : 1;
  }

  for (size_t r{1}; r < t.m_recs.size(); ++r) {
    const auto prev = key_copy(index, t.m_recs[r - 1], heap);
    ulint matched;

    (void)compare(index, prev, t.m_recs[r], matched);

    for (ulint j{matched + 1}; j <= n_uniq; ++j) {
      ++i->m_stats.m_n_diff_key_vals[j];
    }

    mem_heap_empty(heap);
  }

  mem_heap_free(heap);
}

std::vector<const DTuple *> MemAm::split_range(const Index *index, const DTuple *start, const DTuple *end, size_t n, MemHeap *heap) noexcept {
  auto &t = tree(index);
  std::lock_guard lock{t.m_mutex};
  std::vector<const DTuple *> keys;

  /* No more parts than the "pages" of n_pages(): an index of one has nothing to split. */
  n = std::min(n, t.m_recs.size() / RECS_PER_PAGE);

  if (n < 2) {
    return keys;
  }

  const auto step = t.m_recs.size() / n;

  for (size_t i{1}; i < n; ++i) {
    const auto rec = t.m_recs[i * step];
    ulint matched;

    if ((start == nullptr || compare(index, start, rec, matched) < 0) && (end == nullptr || compare(index, end, rec, matched) > 0)) {
      keys.push_back(key_copy(index, rec, heap));
    }
  }

  return keys;
}

}  // namespace

std::unique_ptr<IndexAm> index_am_mem(System &sys) { return std::make_unique<MemAm>(sys); }
