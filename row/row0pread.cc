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
Parallel read implementation: by key ranges of an index and its cursors, see row0pread.h.

Created 2018-01-27 by Sunny Bains */

#include <array>

#include "dict0dict.h"
#include "idx0am.h"
#include "lock0lock.h"
#include "os0thread-create.h"
#include "row0pread.h"
#include "row0row.h"
#include "row0sel.h"
#include "row0vers.h"
#include "srv0sys.h"
#include "ut0logger.h"

/** The parts of a range for each thread: more than one, so that a thread that finishes early takes
another part instead of waiting for the slowest one. */
static constexpr size_t PARTS_PER_THREAD{4};

/** Records read in one mini-transaction: then it's committed with the cursor saved, and the scan
holds nothing for long. */
static constexpr size_t BATCH_SIZE{1024};

/** Batches read, in large ranges, before the check for trx interrupted, which is expensive. */
static constexpr size_t TRX_IS_INTERRUPTED_PROBE{50};

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

ParallelReader::~ParallelReader() {
  mutex_free(&m_mutex);
  CondVar::destroy(m_event);
  for (auto thread_ctx : m_thread_ctxs) {
    if (thread_ctx != nullptr) {
      delete thread_ctx;
    }
  }
}

size_t ParallelReader::available_threads(System &sys, size_t n_required, bool use_reserved) {
  auto &s_active_threads = sys.m_srv->m_n_parallel_read_threads;

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

ParallelReader::ParallelReader(size_t max_threads)
    : m_max_threads(max_threads), m_n_threads(max_threads), m_ctxs(), m_sync(max_threads == 0) {
  m_n_completed = 0;

  mutex_create(&m_mutex, IF_DEBUG("parallel_read_mutex", ) Current_location());

  m_event = CondVar::create("parallel_reader");
  m_sig_count = m_event->reset();
}

ParallelReader::ScanCtx::ScanCtx(ParallelReader *reader, size_t id, Trx *trx, const ParallelReader::Config &config, F &&f)
    : m_id(id), m_config(config), m_trx(trx), m_f(f), m_reader(reader), m_heap(mem_heap_create(1024)) {}

ParallelReader::ScanCtx::~ScanCtx() noexcept { mem_heap_free(m_heap); }

void ParallelReader::ThreadCtx::savepoint(System &sys) noexcept {
  ut_a(m_cur != nullptr && m_mtr != nullptr);

  m_cur->save(m_mtr);
  m_cur->commit(m_mtr);

  (void)sys;
}

DbErr ParallelReader::ThreadCtx::restore_from_savepoint(System &) noexcept {
  ut_a(m_cur != nullptr && m_mtr != nullptr);

  m_mtr->start();
  m_mtr->disable_redo_logging();

  /* Back on the record, or before where it was if it's gone: the next record is the next to read. */
  (void)m_cur->restore(Intent::Read, m_mtr);

  return DB_SUCCESS;
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
    } else if (m_trx->m_isolation_level > TRX_ISO_READ_UNCOMMITTED && !sys.m_lock_sys->sec_rec_cons_read_sees(rec, view)) {
      /* A transaction the view doesn't see changed the page: the snapshot
      decides through the clustered index, whatever the delete mark of rec. */
      return sec_rec_visible(sys, rec, heap, mtr);
    }
  }

  if (rec.get_deleted_flag()) {
    /* This record was deleted in the latest committed version, or it was
    deleted and then reinserted-by-update before purge kicked in. Skip it. */
    return false;
  }

  ut_ad(m_trx == nullptr || m_trx->m_isolation_level == TRX_ISO_READ_UNCOMMITTED || rec.offs_any_null_extern(offsets) == nullptr);

  return true;
}

bool ParallelReader::ScanCtx::sec_rec_visible(System &sys, const Rec rec, MemHeap *&heap, Mtr *) {
  auto sec_index = m_config.m_index;
  auto clust_index = sec_index->m_table->get_clustered_index();
  auto view = m_trx->m_read_view;

  /* The row of rec in the clustered index: its primary key is in rec, which stays where it is in the
  mini-transaction of the scan while the reference is used. */
  auto ref = row_build_row_ref(ROW_COPY_POINTERS, sec_index, rec, heap);

  IndexCursor cur{sys};

  /* The clustered index in a mini-transaction of its own, released before the next record: kept in
  the scan's until the scan leaves the page, a later lookup of the same page would wait for (and
  deadlock with) a writer waiting for it. */
  Mtr clust_mtr;

  clust_mtr.start();

  if (!cur.find(clust_index, ref, Intent::Read, &clust_mtr)) {
    /* The row is gone (a rolled back insert, purge is removing rec): it
    did not exist in the view. */
    cur.commit(&clust_mtr);
    return false;
  }

  Rec clust_rec = cur.rec();

  RecOffs clust_offsets{};

  {
    PhyRec record{clust_index, clust_rec};

    clust_offsets = record.get_all_col_offsets(nullptr, &heap, Current_location());
  }

  /* The version of the row in the snapshot. */
  if (!sys.m_lock_sys->clust_rec_cons_read_sees(clust_rec, clust_index, clust_offsets, view)) {
    Rec old_vers;

    RowVers::Row row{
      .m_cluster_rec = clust_rec,
      .m_mtr = &clust_mtr,
      .m_cluster_index = clust_index,
      .m_cluster_offsets = clust_offsets,
      .m_consistent_read_view = view,
      .m_cluster_offset_heap = heap,
      .m_old_row_heap = heap,
      .m_old_rec = old_vers
    };

    const auto err = sys.m_row_vers->build_for_consistent_read(row);
    ut_a(err == DB_SUCCESS);

    clust_rec = old_vers;
  }

  /* rec is visible if the row existed in the snapshot, was not deleted there,
  and its secondary index entry was rec: rec may be the entry of another
  version of the row, before or after an update of the indexed columns. */
  const auto visible =
    !clust_rec.is_null() && !clust_rec.get_deleted_flag() && sys.m_row_sel->sec_rec_is_for_clust_rec(rec, sec_index, clust_rec, clust_index);

  cur.commit(&clust_mtr);

  return visible;
}

DbErr ParallelReader::Ctx::traverse(System &sys) {
  const auto index = m_scan_ctx->m_config.m_index;
  const auto start = m_range.first;
  const auto end = m_range.second;
  const auto &start_cb = m_scan_ctx->m_reader->m_start_callback;
  const auto &finish_cb = m_scan_ctx->m_reader->m_finish_callback;
  auto heap = mem_heap_create(UNIV_PAGE_SIZE / 4);

  Mtr mtr;
  IndexCursor cur{sys};

  mtr.start();
  mtr.disable_redo_logging();

  m_thread_ctx->m_cur = &cur;
  m_thread_ctx->m_mtr = &mtr;

  /* On the first record of the range. */
  bool on;

  if (start != nullptr) {
    cur.seek(const_cast<Index *>(index), start, PAGE_CUR_GE, Intent::Read, &mtr);
    on = cur.on_rec();
  } else {
    cur.seek_first(index, Intent::Read, &mtr);
    on = cur.next(&mtr);
  }

  m_start = true;

  DbErr err{DB_SUCCESS};

  /* The start of a batch: m_first_rec until its first record. */
  const auto batch_start = [&]() {
    m_first_rec = true;

    if (start_cb) {
      m_thread_ctx->m_state = State::BATCH;
      return start_cb(m_thread_ctx);
    }

    return DB_SUCCESS;
  };

  const auto batch_finish = [&]() {
    ++m_n_batches;

    if (finish_cb) {
      m_thread_ctx->m_state = State::BATCH;
      return finish_cb(m_thread_ctx);
    }

    return DB_SUCCESS;
  };

  err = batch_start();

  /* A batch is open: started and not finished. The cursor is held: positioned, in mtr. */
  bool in_batch{true};
  bool held{true};

  for (size_t n{}; on && err == DB_SUCCESS; on = cur.next(&mtr)) {
    std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
    auto offsets = offsets_.data();

    rec_offs_init(offsets_);

    Rec rec = cur.rec();

    {
      PhyRec record{index, rec};

      offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
    }

    /* The key of a record changes only by a delete and an insert, the record keeps its key with the
    delete mark: the latest version says whether it's in the range. The bound itself can be a key
    that's gone, the parts aren't made under any view. */
    if (end != nullptr && end->compare(rec, index, offsets) <= 0) {
      break;
    }

    RecOffs rec_offsets(offsets);

    if (m_scan_ctx->check_visibility(sys, rec, rec_offsets, heap, &mtr)) {
      m_rec = rec;
      m_offsets = rec_offsets;

      err = m_scan_ctx->m_f(this);

      if (err != DB_SUCCESS) {
        break;
      }

      m_start = false;
    }

    m_first_rec = false;

    mem_heap_empty(heap);

    if (++n % BATCH_SIZE == 0) {
      /* The end of a batch: the cursor saved, nothing held, then on from where it was. */
      in_batch = false;

      if ((err = batch_finish()) != DB_SUCCESS) {
        break;
      }

      m_thread_ctx->savepoint(sys);
      held = false;

      if (m_n_batches % TRX_IS_INTERRUPTED_PROBE == 0 && trx()->is_interrupted()) {
        err = DB_INTERRUPTED;
        break;
      }

      if (is_error_set()) {
        break;
      }

      (void)m_thread_ctx->restore_from_savepoint(sys);
      held = true;

      in_batch = true;

      if ((err = batch_start()) != DB_SUCCESS) {
        break;
      }
    }
  }

  if (err != DB_SUCCESS) {
    m_scan_ctx->set_error_state(err);
  }

  /* The batch that is open: started, and not finished above. */
  if (in_batch) {
    if (const auto cb_err = batch_finish(); cb_err != DB_SUCCESS && err == DB_SUCCESS) {
      err = cb_err;
      m_scan_ctx->set_error_state(err);
    }
  }

  /* Saved and not back: it holds nothing, and its mini-transaction is committed. */
  if (held) {
    cur.commit(&mtr);
  }

  m_thread_ctx->m_cur = nullptr;
  m_thread_ctx->m_mtr = nullptr;

  mem_heap_free(heap);

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

void ParallelReader::worker(System &sys, ParallelReader::ThreadCtx *thread_ctx) {
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

      if (m_start_callback) {
        /* Context start. */
        thread_ctx->m_state = State::CTX;
        cb_err = m_start_callback(thread_ctx);
      }

      if (cb_err == DB_SUCCESS && err == DB_SUCCESS) {
        err = ctx->traverse(sys);
      }

      if (m_finish_callback) {
        /* Context finished. */
        thread_ctx->m_state = State::CTX;
        cb_err = m_finish_callback(thread_ctx);
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

DbErr ParallelReader::ScanCtx::partition(System &sys) {
  const auto index = m_config.m_index;
  const auto &range = m_config.m_scan_range;
  const auto n = std::max(max_threads(), size_t{1}) * PARTS_PER_THREAD;

  /* The keys the index finds in the range, in order and inside it: [start, k1), [k1, k2) ... [kn, end).
  An empty index has a range too: its scan finds nothing. */
  const auto keys = sys.am(index).split_range(index, range.m_start, range.m_end, n, m_heap);

  /* The bounds are the scan's to keep: the caller's tuples are its own. */
  const auto copy = [&](const DTuple *t) -> const DTuple * { return t == nullptr ? nullptr : dtuple_copy(t, m_heap); };

  auto lower = copy(range.m_start);

  for (const auto key : keys) {
    if (const auto err = create_context(Range{lower, key}); err != DB_SUCCESS) {
      return err;
    }

    lower = key;
  }

  return create_context(Range{lower, copy(range.m_end)});
}

DbErr ParallelReader::ScanCtx::create_context(const Range &range) {
  auto ctx_id = m_reader->m_ctx_id.fetch_add(1, std::memory_order_relaxed);

  auto ctx = std::shared_ptr<Ctx>(new (std::nothrow) Ctx(ctx_id, this, range), [](Ctx *ctx) { delete ctx; });

  if (ctx.get() == nullptr) {
    m_reader->m_ctx_id.fetch_sub(1, std::memory_order_relaxed);
    return DB_OUT_OF_MEMORY;
  }

  m_reader->enqueue(ctx);

  return DB_SUCCESS;
}

void ParallelReader::parallel_read(System &sys) {
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

    worker(sys, m_thread_ctxs[0]);

    return;
  }

  ut_a(m_n_threads > 0);

  m_thread_ctxs.reserve(m_n_threads);

  for (size_t i = 0; i < m_n_threads; ++i) {
    try {
      auto ptr = new (std::nothrow) ThreadCtx(i);

      if (ptr == nullptr) {
        set_error_state(DB_OUT_OF_MEMORY);
        return;
      }

      m_thread_ctxs.emplace_back(ptr);

      m_parallel_read_threads.emplace_back(create_joinable_thread(&ParallelReader::worker, this, std::ref(sys), m_thread_ctxs[i]));

    } catch (...) {
      /* Set the global error state to tell the worker threads to exit. */
      set_error_state(DB_OUT_OF_RESOURCES);
      break;
    }
  }

  m_event->set();
}

DbErr ParallelReader::spawn(System &sys, size_t n_threads) noexcept {
  /* In case this is a retry after a DB_OUT_OF_RESOURCES error. */
  m_err = DB_SUCCESS;

  m_n_threads = n_threads;

  if (max_threads() > m_n_threads) {
    release_unused_threads(sys, max_threads() - m_n_threads);
  }

  parallel_read(sys);

  return DB_SUCCESS;
}

DbErr ParallelReader::run(System &sys, size_t n_threads) {
  /* In case this is a retry after a DB_OUT_OF_RESOURCES error. */
  m_err = DB_SUCCESS;

  ut_a(max_threads() >= n_threads);

  if (n_threads == 0) {
    m_sync = true;
  }

  const auto err = spawn(sys, n_threads);

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

    /* The worker threads have exited: give them back to the instance. */
    release_unused_threads(sys, m_n_threads);
    m_n_threads = 0;

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

DbErr ParallelReader::add_scan(System &sys, Trx *trx, const ParallelReader::Config &config, ParallelReader::F &&f) {
  auto scan_ctx = std::make_shared<ScanCtx>(this, m_scan_ctx_id, trx, config, std::move(f));

  m_scan_ctxs.push_back(scan_ctx);

  ++m_scan_ctx_id;

  return scan_ctx->partition(sys);
}
