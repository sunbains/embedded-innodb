/*****************************************************************************
Copyright (c) 2018, 2021, Oracle and/or its affiliates.
Copyright (c) 2024 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file row/row0pread.cc
Parallel read implementation

Created 2018-01-27 by Sunny Bains */

#include <array>

#include "btr0pcur.h"
#include "dict0dict.h"
#include "os0thread-create.h"
#include "row0pread.h"
#include "row0row.h"
#include "row0vers.h"
#include "ut0logger.h"

std::atomic_size_t ParallelReader::s_active_threads{};

/** Tree depth at which we decide to split blocks further. */
static constexpr size_t SPLIT_THRESHOLD{3};

/** No. of pages to scan, in the case of large tables, before the check for
trx interrupted is made as the call is expensive. */
static constexpr size_t TRX_IS_INTERRUPTED_PROBE{50000};

std::string ParallelReader::ScanRange::to_string() const {
  std::ostringstream os;

  os << "m_start: ";
  if (m_start != nullptr) {
    m_start->print(os);
  } else {
    os << "null";
  }
  os << ", m_end: ";
  if (m_end != nullptr) {
    m_end->print(os);
  } else {
    os << "null";
  }
  return os.str();
}

ParallelReader::ScanCtx::Iter::~Iter() {
  if (m_heap == nullptr) {
    return;
  }

  if (m_pcur != nullptr) {
    m_pcur->free_rec_buf();
    /* Created with placement new on the heap. */
    call_destructor(m_pcur);
  }

  mem_heap_free(m_heap);
  m_heap = nullptr;
}

ParallelReader::~ParallelReader() {
  mutex_free(&m_mutex);
  CondVar::destroy(m_event);
  if (!m_sync) {
    release_unused_threads(m_n_threads);
  }
  for (auto thread_ctx : m_thread_ctxs) {
    if (thread_ctx != nullptr) {
      delete thread_ctx;
    }
  }
}

size_t ParallelReader::available_threads(size_t n_required, bool use_reserved) {
  auto max_threads = MAX_THREADS;
  constexpr auto SEQ_CST = std::memory_order_seq_cst;
  auto active = s_active_threads.fetch_add(n_required, SEQ_CST);

  if (use_reserved) {
    max_threads += MAX_RESERVED_THREADS;
  }

  if (active < max_threads) {
    const auto available = max_threads - active;

    if (n_required <= available) {
      return n_required;
    } else {
      const auto release = n_required - available;
      const auto o = s_active_threads.fetch_sub(release, SEQ_CST);
      ut_a(o >= release);
      return available;
    }
  }

  const auto o = s_active_threads.fetch_sub(n_required, SEQ_CST);
  ut_a(o >= n_required);

  return 0;
}

void ParallelReader::ScanCtx::index_s_lock() {
  if (m_s_locks.fetch_add(1, std::memory_order_acquire) == 0) {
    auto index = m_config.m_index;
    /* The latch can be unlocked by a thread that didn't originally lock it. */
    rw_lock_s_lock_gen(index->get_lock(), true);
  }
}

void ParallelReader::ScanCtx::index_s_unlock() {
  if (m_s_locks.fetch_sub(1, std::memory_order_acquire) == 1) {
    auto index = m_config.m_index;
    /* The latch can be unlocked by a thread that didn't originally lock it. */
    rw_lock_s_unlock_gen(index->get_lock(), true);
  }
}

DbErr ParallelReader::Ctx::split() {
  ut_ad(m_range.first->m_tuple == nullptr || dtuple_validate(m_range.first->m_tuple));
  ut_ad(m_range.second->m_tuple == nullptr || dtuple_validate(m_range.second->m_tuple));

  /* Setup the sub-range. */
  ScanRange scan_range(m_range.first->m_tuple, m_range.second->m_tuple);

  /* S lock so that the tree structure doesn't change while we are
  figuring out the sub-trees to scan. */
  m_scan_ctx->index_s_lock();

  ParallelReader::ScanCtx::Ranges ranges{};
  m_scan_ctx->partition(scan_range, ranges, 1);

  if (!ranges.empty()) {
    ranges.back().second = m_range.second;
  }

  DbErr err{DB_SUCCESS};

  /* Create the partitioned scan execution contexts. */
  for (auto &range : ranges) {
    err = m_scan_ctx->create_context(range, false);

    if (err != DB_SUCCESS) {
      break;
    }
  }

  if (err != DB_SUCCESS) {
    m_scan_ctx->set_error_state(err);
  }

  m_scan_ctx->index_s_unlock();

  return err;
}

ParallelReader::ParallelReader(size_t max_threads)
    : m_max_threads(max_threads), m_n_threads(max_threads), m_ctxs(), m_sync(max_threads == 0) {
  m_n_completed = 0;

  mutex_create(&m_mutex, IF_DEBUG("parallel_read_mutex", ) IF_SYNC_DEBUG(SYNC_PARALLEL_READ, ) Current_location());

  m_event = CondVar::create("parallel_reader");
  m_sig_count = m_event->reset();
}

ParallelReader::ScanCtx::ScanCtx(ParallelReader *reader, size_t id, Trx *trx, const ParallelReader::Config &config, F &&f)
    : m_id(id), m_config(config), m_trx(trx), m_f(f), m_reader(reader) {}

/** Persistent cursor wrapper around btr_pcur_t */
class PCursor {
 public:
  /** Constructor.
  @param[in,out] pcur           Persistent cursor in use.
  @param[in] mtr                Mini-transaction used by the persistent cursor.
  @param[in] read_level         Read level where the block should be present. */
  PCursor(BtreePcursor *pcur, Mtr *mtr, size_t read_level) : m_mtr(mtr), m_pcur(pcur), m_read_level(read_level) {}

  /** Create a savepoint and commit the mini-transaction.*/
  void savepoint(System &sys) noexcept;

  /** Resume from savepoint. */
  void resume(System &sys) noexcept;

  /** Move to the next block.
  @param[in]  index             Index being traversed.
  @return DB_SUCCESS or error code. */
  [[nodiscard]] DbErr move_to_next_block(System &sys, Index *index);

  /** Restore the cursor position. */
  void restore_position(System &sys) noexcept {
    constexpr auto MODE = BTR_SEARCH_LEAF;
    const auto relative = m_pcur->m_rel_pos;
    auto equal = m_pcur->restore_position(sys, MODE, m_mtr, Current_location());

    switch (relative) {
      case BtreeCursorPos::ON:
        if (!equal) {
          m_pcur->get_page_cur()->move_to_next();
        }
        break;

      case BtreeCursorPos::UNSET:
      case BtreeCursorPos::BEFORE_FIRST_IN_TREE:
        ut_error;
        break;

      case BtreeCursorPos::AFTER:
      case BtreeCursorPos::AFTER_LAST_IN_TREE:
        break;

      case BtreeCursorPos::BEFORE:
        /* For non-optimistic restoration:
        The position is now set to the record before pcur->old_rec.

        For optimistic restoration:
        The position also needs to take the previous search_mode into
        consideration. */
        switch (m_pcur->m_pos_state) {
          case BtrPcurPositioned::IS_POSITIONED_OPTIMISTIC:
            m_pcur->m_pos_state = BtrPcurPositioned::IS_POSITIONED;
            /* The cursor always moves "up" ie. in ascending order. */
            break;

          case BtrPcurPositioned::IS_POSITIONED:
            if (m_pcur->is_on_user_rec()) {
              (void)m_pcur->move_to_next(sys, m_mtr);
            }
            break;

          case BtrPcurPositioned::UNSET:
          case BtrPcurPositioned::WAS_POSITIONED:
            ut_error;
        }
        break;
    }
  }

  /** @return the current page cursor. */
  [[nodiscard]] PageCursor *get_page_cursor() noexcept { return m_pcur->get_page_cur(); }

  /** Restore from a saved position.
  @return DB_SUCCESS or error code. */
  [[nodiscard]] DbErr restore_from_savepoint(System &sys) noexcept;

  /** Move to the first user rec on the restored page. */
  [[nodiscard]] DbErr move_to_user_rec(System &sys) noexcept;

  /** @return true if cursor is after last on page. */
  [[nodiscard]] bool is_after_last_on_page() const noexcept { return m_pcur->is_after_last_on_page(); }

 private:
  /** Mini-transaction. */
  Mtr *m_mtr{};

  /** Persistent cursor. */
  BtreePcursor *m_pcur{};

  /** Level where the cursor is positioned or need to be positioned in case of
  restore. */
  size_t m_read_level{};
};

BufBlock *ParallelReader::ScanCtx::block_get_s_latched(System &sys, const PageId &page_id, Mtr *mtr, ulint line) const {
  /* We never scan undo tablespaces. */
  BufPool::Request req{
    .m_rw_latch = RW_S_LATCH, .m_page_id = page_id, .m_mode = BUF_GET, .m_file = __FILE__, .m_line = ulint(line), .m_mtr = mtr
  };

  auto block = sys.m_buf_pool->get(req, nullptr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TREE_NODE));

  return block;
}

void PCursor::savepoint(System &sys) noexcept {
  /* Store the cursor position on the previous user record on the page. */
  m_pcur->move_to_prev_on_page(sys);

  m_pcur->store_position(sys, m_mtr);

  m_mtr->commit(sys);
}

void PCursor::resume(System &sys) noexcept {
  m_mtr->start();

  m_mtr->disable_redo_logging();

  /* Restore position on the record, or its predecessor if the record
  was purged meanwhile. */

  restore_position(sys);

  if (!m_pcur->is_after_last_on_page()) {
    /* Move to the successor of the saved record. */
    m_pcur->move_to_next_on_page(sys);
  }
}

DbErr PCursor::move_to_user_rec(System &sys) noexcept {
  auto cur = m_pcur->get_page_cur();
  const auto next_page_no = Btree::page_get_next(cur->get_page(), m_mtr);

  if (next_page_no == FIL_NULL) {
    m_mtr->commit(sys);
    return DB_END_OF_INDEX;
  }

  auto block = cur->get_block();
  const PageId page_id{block->get_space(), block->get_page_no()};

  BufPool::Request req{
    .m_rw_latch = RW_S_LATCH,
    .m_page_id = {page_id.m_space_id, next_page_no},
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = m_mtr
  };

  block = sys.m_buf_pool->get(req, nullptr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TREE_NODE));

  if (page_is_leaf(block->get_frame())) {
    sys.m_btree->leaf_page_release(cur->get_block(), RW_S_LATCH, m_mtr);
  }

  cur->set_before_first(block);

  /* Skip the infimum record. */
  cur->move_to_next();

  /* Page can't be empty unless it is a root page. */
  ut_ad(!cur->is_after_last());

  return DB_SUCCESS;
}

DbErr PCursor::restore_from_savepoint(System &sys) noexcept {
  resume(sys);
  return m_pcur->is_on_user_rec() ? DB_SUCCESS : move_to_user_rec(sys);
}

DbErr ParallelReader::ThreadCtx::restore_from_savepoint(System &sys) noexcept {
  return m_pcursor->restore_from_savepoint(sys);
}

void ParallelReader::ThreadCtx::savepoint(System &sys) noexcept {
  m_pcursor->savepoint(sys);
}

DbErr PCursor::move_to_next_block(System &sys, Index *index) {
  ut_ad(m_pcur->is_after_last_on_page());

  if (rw_lock_get_waiters(index->get_lock())) {
    /* There are waiters on the index tree lock. Store and restore
    the cursor position, and yield so that scanning a large table
    will not starve other threads. */

    /* We should always yield on a block boundary. */
    ut_ad(m_pcur->is_after_last_on_page());

    savepoint(sys);

    /* Yield so that another thread can proceed. */
    std::this_thread::yield();

    return restore_from_savepoint(sys);
  } else {
    return move_to_user_rec(sys);
  }
}

bool ParallelReader::ScanCtx::check_visibility(System &sys, Rec &rec, RecOffs &offsets, MemHeap *&heap, Mtr *mtr) {
  ut_ad(m_trx == nullptr || m_trx->m_read_view != nullptr);

  if (m_trx != nullptr && m_trx->m_read_view != nullptr) {
    auto view = m_trx->m_read_view;

    if (m_config.m_index->is_clustered()) {
      trx_id_t trx_id;

      if (m_config.m_index->m_trx_id_offset > 0) {
        trx_id = TrxSys::read_trx_id(rec + m_config.m_index->m_trx_id_offset);
      } else {
        trx_id = row_get_rec_trx_id(rec, m_config.m_index, offsets);
      }

      if (m_trx->m_isolation_level > TRX_ISO_READ_UNCOMMITTED && !view->changes_visible(trx_id)) {
        Rec old_vers;

        RowVers::Row row{
          .m_cluster_rec = rec,
          .m_mtr = mtr,
          .m_cluster_index = m_config.m_index,
          .m_cluster_offsets = offsets,
          .m_consistent_read_view = view,
          .m_cluster_offset_heap = heap,
          .m_old_row_heap = heap,
          .m_old_rec = old_vers
        };

        auto err = sys.m_row_vers->build_for_consistent_read(row);
        ut_a(err == DB_SUCCESS);

        rec = old_vers;

        if (rec.is_null()) {
          return false;
        }
      }
    } else {
      /* Secondary index scan not supported yet. */
      ut_error;
    }
  }

  if (rec.get_deleted_flag()) {
    /* This record was deleted in the latest committed version, or it was
    deleted and then reinserted-by-update before purge kicked in. Skip it. */
    return false;
  }

  ut_ad(m_trx == nullptr || m_trx->m_isolation_level == TRX_ISO_READ_UNCOMMITTED || !rec_offs_any_extern(offsets));

  return true;
}

void ParallelReader::ScanCtx::copy_row(const Rec rec, Iter *iter) const {
  {
    PhyRec record{m_config.m_index, rec};

    iter->m_offsets = record.get_all_col_offsets(nullptr, &iter->m_heap, Current_location());
  }

  /* Copy the row from the page to the scan iterator. The copy should use
  memory from the iterator heap because the scan iterator owns the copy. */
  auto rec_len = iter->m_offsets.size();

  auto copy_rec = static_cast<Rec>(mem_heap_alloc(iter->m_heap, rec_len));

  memcpy(copy_rec, rec.get(), rec_len);

  iter->m_rec = copy_rec;

  ulint n_ext{};

  auto tuple = row_rec_to_index_entry_low(iter->m_rec, m_config.m_index, iter->m_offsets, &n_ext, iter->m_heap);

  ut_ad(dtuple_validate(tuple));

  /* We have copied the entire record but we only need to compare the
  key columns when we check for boundary conditions. */
  const auto n_compare = m_config.m_index->get_n_unique_in_tree();

  dtuple_set_n_fields_cmp(tuple, n_compare);

  iter->m_tuple = tuple;
}

std::shared_ptr<ParallelReader::ScanCtx::Iter> ParallelReader::ScanCtx::create_persistent_cursor(System &sys, 
  const PageCursor &page_cursor, Mtr *mtr
) const {
  ut_ad(index_s_own());

  std::shared_ptr<Iter> iter = std::make_shared<Iter>();

  iter->m_heap = mem_heap_create(sizeof(BtreePcursor) + (UNIV_PAGE_SIZE / 16));

  auto rec = page_cursor.m_rec;

  const bool is_infimum = page_rec_is_infimum(rec);

  if (is_infimum) {
    rec = page_rec_get_next(rec);
  }

  if (page_rec_is_supremum(rec)) {
    /* Empty page, only root page can be empty. */
    ut_a(!is_infimum || page_cursor.m_block->get_page_no() == m_config.m_index->m_page_id.m_page_no);
    return iter;
  }

  void *ptr = mem_heap_alloc(iter->m_heap, sizeof(BtreePcursor));

  ::new (ptr) BtreePcursor(sys.m_fsp, sys.m_btree);

  iter->m_pcur = reinterpret_cast<BtreePcursor *>(ptr);

  iter->m_pcur->init(m_config.m_read_level);

  /* Make a copy of the rec. */
  copy_row(rec, iter.get());

  iter->m_pcur->open_on_user_rec(page_cursor, PAGE_CUR_GE, BTR_ALREADY_S_LATCHED | BTR_SEARCH_LEAF);

  ut_ad(sys.m_btree->page_get_level(iter->m_pcur->get_block()->get_frame(), mtr) == m_config.m_read_level);

  iter->m_pcur->store_position(mtr);

  return iter;
}

bool ParallelReader::Ctx::move_to_next_node(System &sys, PCursor *pcursor) {
  IF_DEBUG(auto cur = m_range.first->m_pcur->get_page_cur();)

  auto err = pcursor->move_to_next_block(sys, const_cast<Index *>(index()));

  if (err != DB_SUCCESS) {
    ut_a(err == DB_END_OF_INDEX);
    return false;
  } else {
    /* Page can't be empty unless it is a root page. */
    ut_ad(!cur->is_after_last());
    ut_ad(!cur->is_before_first());
    return true;
  }
}

DbErr ParallelReader::Ctx::traverse(System &sys) {
  /* Take index lock if the requested read level is on a non-leaf level as the
  index lock is required to access non-leaf page.  */
  if (m_scan_ctx->m_config.m_read_level != 0) {
    m_scan_ctx->index_s_lock();
  }

  Mtr mtr;

  mtr.start();

  mtr.disable_redo_logging();

  auto &from = m_range.first;

  PCursor pcursor(from->m_pcur, &mtr, m_scan_ctx->m_config.m_read_level);
  pcursor.restore_position(sys);

  DbErr err{DB_SUCCESS};

  m_thread_ctx->m_pcursor = &pcursor;

  err = traverse_recs(&pcursor, &mtr);

  if (mtr.is_active()) {
    mtr.commit(sys);
  }

  m_thread_ctx->m_pcursor = nullptr;

  if (m_scan_ctx->m_config.m_read_level != 0) {
    m_scan_ctx->index_s_unlock();
  }

  return err;
}

DbErr ParallelReader::Ctx::traverse_recs(System &sys, PCursor *pcursor, Mtr *mtr) {
  const auto &end_tuple = m_range.second->m_tuple;
  auto heap = mem_heap_create(UNIV_PAGE_SIZE / 4);
  auto index = m_scan_ctx->m_config.m_index;

  m_start = true;

  DbErr err{DB_SUCCESS};

  if (m_scan_ctx->m_reader->m_start_callback) {
    /* Page start. */
    m_thread_ctx->m_state = State::PAGE;
    err = m_scan_ctx->m_reader->m_start_callback(m_thread_ctx);
  }

  bool call_end_page{true};
  auto cur = pcursor->get_page_cursor();

  while (err == DB_SUCCESS) {
    if (cur->is_after_last()) {
      call_end_page = false;

      if (m_scan_ctx->m_reader->m_finish_callback) {
        /* End of page. */
        m_thread_ctx->m_state = State::PAGE;
        err = m_scan_ctx->m_reader->m_finish_callback(m_thread_ctx);
        if (err != DB_SUCCESS) {
          break;
        }
      }

      mem_heap_empty(heap);

      if (!(m_n_pages % TRX_IS_INTERRUPTED_PROBE) && trx()->is_interrupted()) {
        err = DB_INTERRUPTED;
        break;
      }

      if (is_error_set()) {
        break;
      }

      /* Note: The page end callback (above) can save and restore the cursor.
      The restore can end up in the middle of a page. */
      if (pcursor->is_after_last_on_page() && !move_to_next_node(pcursor)) {
        break;
      }

      ++m_n_pages;
      m_first_rec = true;

      call_end_page = true;

      if (m_scan_ctx->m_reader->m_start_callback) {
        /* Page start. */
        m_thread_ctx->m_state = State::PAGE;
        err = m_scan_ctx->m_reader->m_start_callback(m_thread_ctx);
        if (err != DB_SUCCESS) {
          break;
        }
      }
    }

    std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
    auto offsets = offsets_.data();

    rec_offs_init(offsets_);

    Rec rec = cur->get_rec();

    {
      PhyRec record{index, rec};

      offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
    }

    if (end_tuple != nullptr) {
      ut_ad(!rec.is_null());

      /* Key value of a record can change only if the record is deleted or if
      it's updated. An update is essentially a delete + insert. So in both
      the cases we just delete mark the record and the original key value is
      preserved on the page.

      Since the range creation is based on the key values and the key values do
      not ever change the, latest (non-MVCC) version of the record should always
      tell us correctly whether we're within the range or outside of it. */
      auto ret = end_tuple->compare(rec, index, offsets);

      /* Note: The range creation doesn't use MVCC. Therefore it's possible
      that the range boundary entry could have been deleted. */
      if (ret <= 0) {
        break;
      }
    }

    bool skip{};

    if (page_is_leaf(cur->m_block->get_frame())) {
      RecOffs rec_offsets(offsets);
      skip = !m_scan_ctx->check_visibility(rec, rec_offsets, heap, mtr);
    }

    if (!skip) {
      m_rec = rec;
      m_offsets = offsets;
      m_block = cur->m_block;

      err = m_scan_ctx->m_f(this);

      if (err != DB_SUCCESS) {
        break;
      }

      m_start = false;
    }

    m_first_rec = false;

    cur->move_to_next(sys);
  }

  if (err != DB_SUCCESS) {
    m_scan_ctx->set_error_state(err);
  }

  mem_heap_free(heap);

  if (call_end_page && m_scan_ctx->m_reader->m_finish_callback) {
    /* Page finished. */
    m_thread_ctx->m_state = State::PAGE;
    auto cb_err = m_scan_ctx->m_reader->m_finish_callback(m_thread_ctx);

    if (cb_err != DB_SUCCESS && !m_scan_ctx->is_error_set()) {
      err = cb_err;
    }
  }

  return err;
}

void ParallelReader::enqueue(std::shared_ptr<Ctx> ctx) {
  mutex_enter(&m_mutex);
  m_ctxs.push_back(ctx);
  mutex_exit(&m_mutex);
}

std::shared_ptr<ParallelReader::Ctx> ParallelReader::dequeue() {
  mutex_enter(&m_mutex);

  if (m_ctxs.empty()) {
    mutex_exit(&m_mutex);
    return nullptr;
  }

  auto ctx = m_ctxs.front();
  m_ctxs.pop_front();

  mutex_exit(&m_mutex);

  return ctx;
}

bool ParallelReader::is_queue_empty() const {
  mutex_enter(&m_mutex);
  auto empty = m_ctxs.empty();
  mutex_exit(&m_mutex);
  return empty;
}

void ParallelReader::worker(ParallelReader::ThreadCtx *thread_ctx) {
  DbErr err{DB_SUCCESS};
  DbErr cb_err{DB_SUCCESS};

  if (m_start_callback) {
    /* Thread start. */
    thread_ctx->m_state = State::THREAD;
    cb_err = m_start_callback(thread_ctx);

    if (cb_err != DB_SUCCESS) {
      err = cb_err;
      set_error_state(cb_err);
    }
  }

  /* Wait for all the threads to be spawned as it's possible that we could
  abort the operation if there are not enough resources to spawn all the
  threads. */
  if (!m_sync) {
    m_event->wait_time(std::chrono::microseconds::max(), m_sig_count);
  }

  for (;;) {
    size_t n_completed{};
    int64_t sig_count = m_event->reset();

    while (err == DB_SUCCESS && cb_err == DB_SUCCESS && !is_error_set()) {
      auto ctx = dequeue();

      if (ctx == nullptr) {
        break;
      }

      auto scan_ctx = ctx->m_scan_ctx;

      if (scan_ctx->is_error_set()) {
        break;
      }

      ctx->m_thread_ctx = thread_ctx;

      if (ctx->m_split) {
        err = ctx->split();
        /* Tell the other threads that there is work to do. */
        m_event->set();
      } else {
        if (m_start_callback) {
          /* Context start. */
          thread_ctx->m_state = State::CTX;
          cb_err = m_start_callback(thread_ctx);
        }

        if (cb_err == DB_SUCCESS && err == DB_SUCCESS) {
          err = ctx->traverse();
        }

        if (m_finish_callback) {
          /* Context finished. */
          thread_ctx->m_state = State::CTX;
          cb_err = m_finish_callback(thread_ctx);
        }
      }

      /* Check for trx interrupted (useful in the case of small tables). */
      if (err == DB_SUCCESS && ctx->trx()->is_interrupted()) {
        err = DB_INTERRUPTED;
        scan_ctx->set_error_state(err);
        break;
      }

      ut_ad(err == DB_SUCCESS || scan_ctx->is_error_set());

      ++n_completed;
    }

    if (cb_err != DB_SUCCESS || err != DB_SUCCESS || is_error_set()) {
      break;
    }

    m_n_completed.fetch_add(n_completed, std::memory_order_relaxed);

    if (m_n_completed == m_ctx_id) {
      /* Wakeup other worker threads before exiting */
      m_event->set();
      break;
    }

    if (!m_sync) {
      m_event->wait_time(std::chrono::microseconds::max(), sig_count);
    }
  }

  if (err != DB_SUCCESS && !is_error_set()) {
    /* Set the "global" error state. */
    set_error_state(err);
  }

  if (is_error_set()) {
    /* Wake up any sleeping threads. */
    m_event->set();
  }

  if (m_finish_callback) {
    /* Thread finished. */
    thread_ctx->m_state = State::THREAD;
    cb_err = m_finish_callback(thread_ctx);

    /* Keep the err status from previous failed operations */
    if (cb_err != DB_SUCCESS) {
      err = cb_err;
      set_error_state(cb_err);
    }
  }

  ut_a(is_error_set() || (m_n_completed == m_ctx_id && is_queue_empty()));
}

page_no_t ParallelReader::ScanCtx::search(System &sys, const BufBlock *block, const DTuple *key) const {
  ut_ad(index_s_own());

  PageCursor page_cursor;
  const auto index = m_config.m_index;

  if (key != nullptr) {
    page_cursor.search(block, index, key, PAGE_CUR_LE);
  } else {
    page_cursor.set_before_first(block);
  }

  if (page_rec_is_infimum(page_cursor.get_rec())) {
    page_cursor.move_to_next();
  }

  const auto rec = page_cursor.get_rec();

  MemHeap *heap = nullptr;

  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();

  rec_offs_init(offsets_);

  {
    PhyRec record{index, rec};

    offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
  }

  auto page_no = sys.m_btree->node_ptr_get_child_page_no(rec, offsets);

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return page_no;
}

PageCursor ParallelReader::ScanCtx::start_range(System &sys, page_no_t page_no, Mtr *mtr, const DTuple *key, Savepoints &savepoints) const {
  ut_ad(index_s_own());

  ulint height{};
  auto index = m_config.m_index;
  PageId page_id(index->m_page_id.m_space_id, page_no);

  /* Follow the left most pointer down on each page. */
  for (;;) {
    auto savepoint = mtr->get_savepoint();

    auto block = block_get_s_latched(page_id, mtr, __LINE__);

    height = sys.m_btree->page_get_level(block->get_frame(), mtr);

    savepoints.push_back({savepoint, block});

    if (height != 0 && height != m_config.m_read_level) {
      page_id.m_page_no = search(block, key);
      continue;
    }

    PageCursor page_cursor;

    if (key != nullptr) {
      page_cursor.search(block, index, key, PAGE_CUR_GE);
    } else {
      page_cursor.set_before_first(block);
    }

    if (page_rec_is_infimum(page_cursor.get_rec())) {
      page_cursor.move_to_next();
    }

    return page_cursor;
  }
}

void ParallelReader::ScanCtx::create_range(Ranges &ranges, PageCursor &leaf_page_cursor, Mtr *mtr) const {
  leaf_page_cursor.m_index = m_config.m_index;

  auto iter = create_persistent_cursor(leaf_page_cursor, mtr);

  /* Setup the previous range (next) to point to the current range. */
  if (!ranges.empty()) {
    ut_a(ranges.back().second->m_heap == nullptr);
    ranges.back().second = iter;
  }

  ranges.push_back(Range(iter, std::make_shared<Iter>()));
}

DbErr ParallelReader::ScanCtx::create_ranges(System &sys, 
  const ScanRange &scan_range, page_no_t page_no, size_t depth, const size_t split_level, Ranges &ranges, Mtr *mtr
) {

  ut_ad(index_s_own());
  ut_a(page_no != FIL_NULL);

  /* Do a breadth first traversal of the B+Tree using recursion. We want to
  set up the scan ranges in one pass. This guarantees that the tree structure
  cannot change while we are creating the scan sub-ranges.

  Once we create the persistent cursor (Range) for a sub-tree we can release
  the latches on all blocks traversed for that sub-tree. */

  const auto index = m_config.m_index;

  PageId page_id(index->m_page_id.m_space_id, page_no);

  Savepoint savepoint({mtr->get_savepoint(), nullptr});

  auto block = block_get_s_latched(page_id, mtr, __LINE__);

  /* read_level requested should be less than the tree height. */
  ut_ad(m_config.m_read_level < sys.m_btree->page_get_level(block->get_frame(), mtr) + 1);

  savepoint.second = block;

  std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
  auto offsets = offsets_.data();

  rec_offs_init(offsets_);

  PageCursor page_cursor;

  page_cursor.m_index = index;

  auto start = scan_range.m_start;

  if (start != nullptr) {
    page_cursor.search(block, index, start, PAGE_CUR_LE);

    if (page_cursor.is_after_last()) {
      return DB_SUCCESS;
    } else if (page_cursor.is_before_first()) {
      page_cursor.move_to_next();
    }
  } else {
    page_cursor.set_before_first(block);
    /* Skip the infimum record. */
    page_cursor.move_to_next();
  }

  MemHeap *heap{};

  const auto at_level = sys.m_btree->page_get_level(block->get_frame(), mtr);

  Savepoints savepoints{};

  while (!page_cursor.is_after_last()) {
    const auto rec = page_cursor.get_rec();

    if (heap == nullptr) {
      heap = mem_heap_create(UNIV_PAGE_SIZE / 4);
    }

    {
      PhyRec record{index, rec};

      offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
    }

    const auto end = scan_range.m_end;

    if (end != nullptr && end->compare(rec, index, offsets) <= 0) {
      break;
    }

    PageCursor level_page_cursor;

    /* Split the tree one level below the root if read_level requested is below the root level. */
    if (at_level > m_config.m_read_level) {
      auto page_no = sys.m_btree->node_ptr_get_child_page_no(rec, offsets);

      if (depth < split_level) {
        /* Need to create a range starting at a lower level in the tree. */
        create_ranges(scan_range, page_no, depth + 1, split_level, ranges, mtr);

        page_cursor.move_to_next();
        continue;
      }

      /* Find the range start in the leaf node. */
      level_page_cursor = start_range(page_no, mtr, start, savepoints);
    } else {
      /* In case of root node being the leaf node or in case we've been asked to
      read the root node (via read_level) place the cursor on the root node and
      proceed. */

      if (start != nullptr) {
        page_cursor.search(block, index, start, PAGE_CUR_GE);
        ut_a(!page_rec_is_infimum(page_cursor.get_rec()));
      } else {
        page_cursor.set_before_first(block);

        /* Skip the infimum record. */
        page_cursor.move_to_next();
        ut_a(!page_cursor.is_after_last());
      }

      /* Since we are already at the requested level use the current page
       cursor. */
      level_page_cursor = page_cursor;
    }

    if (!page_rec_is_supremum(level_page_cursor.get_rec())) {
      create_range(ranges, level_page_cursor, mtr);
    }

    /* We've created the persistent cursor, safe to release S latches on
    the blocks that are in this range (sub-tree). */
    for (auto &savepoint : savepoints) {
      mtr->release_block_at_savepoint(savepoint.first, savepoint.second);
    }

    if (m_depth == 0 && depth == 0) {
      m_depth = savepoints.size();
    }

    savepoints.clear();

    if (at_level == m_config.m_read_level) {
      break;
    }

    start = nullptr;

    page_cursor.move_to_next();
  }

  savepoints.push_back(savepoint);

  for (auto &savepoint : savepoints) {
    mtr->release_block_at_savepoint(savepoint.first, savepoint.second);
  }

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return DB_SUCCESS;
}

DbErr ParallelReader::ScanCtx::partition(System &sys, 
  const ScanRange &scan_range, ParallelReader::ScanCtx::Ranges &ranges, size_t split_level
) {
  ut_ad(index_s_own());

  Mtr mtr;

  mtr.start();

  mtr.disable_redo_logging();

  DbErr err{DB_SUCCESS};

  err = create_ranges(scan_range, m_config.m_index->m_page_id.m_page_no, 0, split_level, ranges, &mtr);

  if (err == DB_SUCCESS && scan_range.m_end != nullptr && !ranges.empty()) {
    auto &iter = ranges.back().second;

    ut_a(iter->m_heap == nullptr);

    iter->m_heap = mem_heap_create(sizeof(BtreePcursor) + (UNIV_PAGE_SIZE / 16));

    iter->m_tuple = dtuple_copy(scan_range.m_end, iter->m_heap);

    /* Do a deep copy. */
    for (size_t i = 0; i < dtuple_get_n_fields(iter->m_tuple); ++i) {
      dfield_dup(&iter->m_tuple->fields[i], iter->m_heap);
    }
  }

  mtr.commit(sys);

  return err;
}

DbErr ParallelReader::ScanCtx::create_context(const Range &range, bool split) {
  auto ctx_id = m_reader->m_ctx_id.fetch_add(1, std::memory_order_relaxed);

  // clang-format off

  auto ctx = std::shared_ptr<Ctx>(new Ctx(ctx_id, this, range), [](Ctx *ctx) { delete ctx; });

  // clang-format on

  DbErr err{DB_SUCCESS};

  if (ctx.get() == nullptr) {
    m_reader->m_ctx_id.fetch_sub(1, std::memory_order_relaxed);
    return DB_OUT_OF_MEMORY;
  } else {
    ctx->m_split = split;
    m_reader->enqueue(ctx);
  }

  return err;
}

DbErr ParallelReader::ScanCtx::create_contexts(const Ranges &ranges) {
  size_t split_point{};

  {
    const auto n = std::max(max_threads(), size_t{1});

    ut_a(n <= ParallelReader::MAX_TOTAL_THREADS);

    if (ranges.size() > n) {
      split_point = (ranges.size() / n) * n;
    } else if (m_depth < SPLIT_THRESHOLD) {
      /* If the tree is not very deep then don't split. For smaller tables
      it is more expensive to split because we end up traversing more blocks*/
      split_point = n;
    }
  }

  size_t i{};

  for (auto range : ranges) {
    auto err = create_context(range, i >= split_point);

    if (err != DB_SUCCESS) {
      return err;
    }

    ++i;
  }

  return DB_SUCCESS;
}

void ParallelReader::parallel_read() {
  if (m_ctxs.empty()) {
    return;
  }

  if (m_sync) {
    auto ptr = new (std::nothrow) ThreadCtx(0);

    if (ptr == nullptr) {
      set_error_state(DB_OUT_OF_MEMORY);
      return;
    }

    m_thread_ctxs.push_back(ptr);

    /* Set event to indicate to ::worker() that no threads will be spawned. */
    m_event->set();

    worker(m_thread_ctxs[0]);

    return;
  }

  ut_a(m_n_threads > 0);

  m_thread_ctxs.reserve(m_n_threads);

  DbErr err{DB_SUCCESS};

  for (size_t i = 0; i < m_n_threads; ++i) {
    try {

      auto ptr = new (std::nothrow) ThreadCtx(i);

      if (ptr == nullptr) {
        set_error_state(DB_OUT_OF_MEMORY);
        return;
      }

      m_thread_ctxs.emplace_back(ptr);

      m_parallel_read_threads.emplace_back(create_joinable_thread(&ParallelReader::worker, this, m_thread_ctxs[i]));

    } catch (...) {
      err = DB_OUT_OF_RESOURCES;
      /* Set the global error state to tell the worker threads to exit. */
      set_error_state(err);
      break;
    }
  }

  m_event->set();
}

DbErr ParallelReader::spawn(size_t n_threads) noexcept {
  /* In case this is a retry after a DB_OUT_OF_RESOURCES error. */
  m_err = DB_SUCCESS;

  m_n_threads = n_threads;

  if (max_threads() > m_n_threads) {
    release_unused_threads(max_threads() - m_n_threads);
  }

  parallel_read();

  return DB_SUCCESS;
}

DbErr ParallelReader::run(size_t n_threads) {
  /* In case this is a retry after a DB_OUT_OF_RESOURCES error. */
  m_err = DB_SUCCESS;

  ut_a(max_threads() >= n_threads);

  if (n_threads == 0) {
    m_sync = true;
  }

  const auto err = spawn(n_threads);

  /* Don't wait for the threads to finish if the read is not synchronous or
  if there's no parallel read. */
  if (m_sync) {
    if (err != DB_SUCCESS) {
      return err;
    }
    ut_a(m_n_threads == 0);
    return is_error_set() ? m_err.load() : DB_SUCCESS;
  } else {
    join();

    if (err != DB_SUCCESS) {
      return err;
    } else if (is_error_set()) {
      return m_err;
    }

    for (auto &scan_ctx : m_scan_ctxs) {
      if (scan_ctx->is_error_set()) {
        /* Return the state of the first Scan context that is in state ERROR. */
        return scan_ctx->m_err;
      }
    }
  }

  return DB_SUCCESS;
}

DbErr ParallelReader::add_scan(Trx *trx, const ParallelReader::Config &config, ParallelReader::F &&f) {
  // clang-format off

  auto scan_ctx = std::make_shared<ScanCtx>(this, m_scan_ctx_id, trx, config, std::move(f));

  // clang-format on

  if (scan_ctx.get() == nullptr) {
    log_fatal("Out of memory");
    return DB_OUT_OF_MEMORY;
  }

  m_scan_ctxs.push_back(scan_ctx);

  ++m_scan_ctx_id;

  scan_ctx->index_s_lock();

  ParallelReader::ScanCtx::Ranges ranges{};
  DbErr err{DB_SUCCESS};

  /* Split at the root node (level == 0). */
  err = scan_ctx->partition(config.m_scan_range, ranges, 0);

  if (ranges.empty() || err != DB_SUCCESS) {
    /* Table is empty. */
    scan_ctx->index_s_unlock();
    return err;
  }

  err = scan_ctx->create_contexts(ranges);

  scan_ctx->index_s_unlock();

  return err;
}
