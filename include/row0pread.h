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

/** @file include/row0pread.h
Parallel read interface.

Created 2018-01-27 by Sunny Bains. */

#pragma once

#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <vector>

#include "os0thread-create.h"
#include "row0sel.h"

// Forward declarations
struct Trx;
struct Mtr;
struct BufBlock;
struct Table;

#include "btr0cur.h"
#include "idx0am.h"
#include "db0err.h"
#include "fil0fil.h"
#include "os0sync.h"
#include "rem0types.h"
#include "ut0mpmcbq.h"


/** A scan of a key range of an index by several threads. The index splits the range in parts by keys
it finds cheaply (IndexAm::split_range(), for a tree the keys of its upper levels), and each part,
[start, end) by key, is scanned by one thread with a cursor of the index, as any reader does: nothing
here is about pages or tree levels, so it reads any implementation of an index. The keys need not be
current: the parts cover the range whatever they are, a stale key only makes the parts uneven.

We want to allow multiple parallel range scans on different indexes at the
same time. To achieve this split out the scan context (ScanCtx) from the
execution context (Ctx). The ScanCtx has the index and transaction
information and the Ctx keeps track of the cursor for a specific thread
during the scan. Each index range scan is added via add_scan(): partitions
are separate indexes, and different indexes and tables can be read by one
instance of a ParallelReader.

A scan commits its mini-transaction every so many records (a batch), with the cursor saved, so that
it holds nothing for long; the callbacks see the start and the end of each batch.

A secondary index is scanned with the visibility of each record decided by
the read view: from the page's max trx id when it says enough, else by the
version of the row in the clustered index. */
class ParallelReader {
 public:
  /** Maximum value for innodb-parallel-read-threads. */
  constexpr static size_t MAX_THREADS{256};

  /** Maximum value for reserved parallel read threads for data load so that
  at least this many threads are always available for data load. */
  constexpr static size_t MAX_RESERVED_THREADS{16};

  /** Maximum value for at most number of parallel read threads that can be
  spawned. */
  constexpr static size_t MAX_TOTAL_THREADS{MAX_THREADS + MAX_RESERVED_THREADS};

  using Links = std::vector<page_no_t>;

  // Forward declaration.
  class Ctx;
  class ScanCtx;
  struct ThreadCtx;

  /** Scan state. */
  enum class State : uint8_t {
    /** Unknown state. */
    UNKNOWN,

    /** Start/Finish thread state. */
    THREAD,

    /** Start/Finish Ctx state. */
    CTX,

    /** Start/Finish of a batch of records, read in one mini-transaction. */
    BATCH
  };

  /** Callback to initialise callers state. */
  using Start = std::function<DbErr(ThreadCtx *thread_ctx)>;

  /** Callback to finalise callers state. */
  using Finish = std::function<DbErr(ThreadCtx *thread_ctx)>;

  /** Callback to process the rows. */
  using F = std::function<DbErr(const Ctx *)>;

  /** Specifies the range from where to start the scan and where to end it. */
  struct ScanRange {
    /** Default constructor. */
    ScanRange() : m_start(), m_end() {}

    /** Copy constructor.
    @param[in] scan_range       Instance to copy from. */
    ScanRange(const ScanRange &scan_range) = default;

    /** Constructor.
    @param[in] start            Start key
    @param[in] end              End key. */
    ScanRange(const DTuple *start, const DTuple *end) : m_start(start), m_end(end) {}

    /** Start of the scan, can be nullptr for -infinity. */
    const DTuple *m_start{};

    /** End of the scan, can be null for +infinity. */
    const DTuple *m_end{};

    /** Convert the instance to a string representation. */
    [[nodiscard]] std::string to_string() const;
  };

  /** Scan (ScanCtx) configuration. */
  struct Config {
    /** Constructor.
    @param[in] scan_range     Range to scan.
    @param[in] index          Cluster index to scan.
    @param[in] partition_id   Partition id if the index to be scanned.
                              belongs to a partitioned table. */
    Config(const ScanRange &scan_range, Index *index, size_t partition_id = std::numeric_limits<size_t>::max())
        : m_scan_range(scan_range), m_index(index), m_partition_id(partition_id) {}

    /** Copy constructor.
    @param[in] config           Instance to copy from. */
    Config(const Config &config)

      = default;

    /** Range to scan. */
    const ScanRange m_scan_range;

    /** (Cluster) Index in table to scan. */
    Index *m_index{};

    /** Partition id if the index to be scanned belongs to a partitioned table,
    else std::numeric_limits<size_t>::max(). */
    size_t m_partition_id{std::numeric_limits<size_t>::max()};
  };

  /** Thread related context information. */
  struct ThreadCtx {
    /** Constructor.
    @param[in]  id  Thread ID */
    explicit ThreadCtx(size_t id) noexcept : m_thread_id(id) {}

    /** Destructor. */
    ~ThreadCtx() noexcept {
      ut_a(m_callback_ctx == nullptr);

      if (m_blob_heap != nullptr) {
        mem_heap_free(m_blob_heap);
      }
    }

    /** Set thread related callback information.
    @param[in]  ctx callback context */
    template <typename T>
    void set_callback_ctx(T *ctx) noexcept {
      ut_ad(m_callback_ctx == nullptr || ctx == nullptr);
      m_callback_ctx = ctx;
    }

    /** Get the thread related callback information/
    @return return context. */
    template <typename T>
    T *get_callback_ctx() noexcept {
      return static_cast<T *>(m_callback_ctx);
    }

    /** Create BLOB heap. */
    void create_blob_heap() noexcept {
      ut_a(m_blob_heap == nullptr);
      m_blob_heap = mem_heap_create(UNIV_PAGE_SIZE);
    }

    /** @return the worker thread state. */
    State get_state() const noexcept { return m_state; }

    /** Save current position, commit any active mtr. */
    void savepoint(System &sys) noexcept;

    /** Restore saved position and resume scan.
    @return DB_SUCCESS or error code. */
    [[nodiscard]] DbErr restore_from_savepoint(System &sys) noexcept;

    /** Thread ID. */
    size_t m_thread_id{std::numeric_limits<size_t>::max()};

    /** Callback information related to the thread.
    @note Needs to be created and destroyed by the callback itself. */
    void *m_callback_ctx{};

    /** BLOB heap per thread. */
    MemHeap *m_blob_heap{};

    /** Worker thread state. */
    State m_state{State::UNKNOWN};

    /** Current persistent cursor. */
    /** The cursor of the range being read, and its mini-transaction: for savepoint(). */
    IndexCursor *m_cur{};
    Mtr *m_mtr{};

    ThreadCtx(ThreadCtx &&) = delete;
    ThreadCtx(const ThreadCtx &) = delete;
    ThreadCtx &operator=(ThreadCtx &&) = delete;
    ThreadCtx &operator=(const ThreadCtx &) = delete;
  };

  /** Constructor.
  @param[in]  max_threads Maximum number of threads to use. */
  explicit ParallelReader(size_t max_threads);

  /** Destructor. */
  ~ParallelReader();

  /** Check how many threads are available for parallel reads.
  @param[in] n_required   Number of threads required.
  @param[in] use_reserved true if reserved threads needs to be considered
  while checking for availability of threads
  @return number of threads available. */
  [[nodiscard]] static size_t available_threads(System &sys, size_t n_required, bool use_reserved);

  /** Release the parallel read threads. */
  static void release_threads(System &sys, size_t n_threads) {
    const auto SEQ_CST = std::memory_order_seq_cst;
    auto active = sys.m_srv->m_n_parallel_read_threads.fetch_sub(n_threads, SEQ_CST);
    ut_a(active >= n_threads);
  }

  /** Add scan context.
  @param[in,out]  trx         Covering transaction.
  @param[in]      config      Scan condfiguration.
  @param[in]      f           Callback function.
  (default is 0 which is leaf level)
  @return error. */
  [[nodiscard]] DbErr add_scan(System &sys, Trx *trx, const Config &config, F &&f);

  /** Wait for the join of threads spawned by the parallel reader. */
  void join() {
    for (auto &thread : m_parallel_read_threads) {
      thread.join();
    }
  }

  /** Get the error stored in the global error state.
  @return global error state. */
  [[nodiscard]] DbErr get_error_state() const { return m_err; }

  /** @return true if the tree is empty, else false. */
  [[nodiscard]] bool is_tree_empty() const { return m_ctx_id.load(std::memory_order_relaxed) == 0; }

  /** Set the callback that must be called before any processing.
  @param[in] f                  Call before first row is processed.*/
  void set_start_callback(Start &&f) { m_start_callback = std::move(f); }

  /** Set the callback that must be called after all processing.
  @param[in] f                  Call after last row is processed.*/
  void set_finish_callback(Finish &&f) { m_finish_callback = std::move(f); }

  /** Spawn the threads to do the parallel read for the specified range.
  Don't wait for the spawned to threads to complete.
  @param[in]  n_threads number of threads that *need* to be spawned
  @return DB_SUCCESS or error code. */
  [[nodiscard]] DbErr spawn(System &sys, size_t n_threads) noexcept;

  /** Start the threads to do the parallel read for the specified range.
  @param[in] n_threads          Number of threads to use for the scan.
  @return DB_SUCCESS or error code. */
  [[nodiscard]] DbErr run(System &sys, size_t n_threads);

  /** @return the configured max threads size. */
  [[nodiscard]] size_t max_threads() const { return m_max_threads; }

  /** @return true if in error state. */
  [[nodiscard]] bool is_error_set() const { return m_err.load(std::memory_order_relaxed) != DB_SUCCESS; }

  /** Set the error state.
  @param[in] err                Error state to set to. */
  void set_error_state(DbErr err) { m_err.store(err, std::memory_order_relaxed); }

  /** Set the number of threads to be spawned.
  @param[in]  n_threads number of threads to be spawned. */
  void set_n_threads(size_t n_threads) {
    ut_ad(n_threads <= m_max_threads);
    m_n_threads = n_threads;
  }

  // Disable copying.
  ParallelReader(const ParallelReader &) = delete;
  ParallelReader(const ParallelReader &&) = delete;
  ParallelReader &operator=(ParallelReader &&) = delete;
  ParallelReader &operator=(const ParallelReader &) = delete;

 private:
  /** Release unused threads back to the pool.
  @param[in] unused_threads     Number of threads to "release". */
  void release_unused_threads(System &sys, size_t unused_threads) {
    ut_a(m_max_threads >= unused_threads);
    release_threads(sys, unused_threads);
  }

  /** Add an execution context to the run queue.
  @param[in] ctx                Execution context to add to the queue. */
  void enqueue(std::shared_ptr<Ctx> ctx);

  /** Fetch the next job execute.
  @return job to execute or nullptr. */
  [[nodiscard]] std::shared_ptr<Ctx> dequeue();

  /** @return true if job queue is empty. */
  [[nodiscard]] bool is_queue_empty() const;

  /** Poll for requests and execute.
  @param[in]  thread_ctx  thread related context information */
  void worker(System &sys, ThreadCtx *thread_ctx);

  /** Create the threads and do a parallel read across the partitions. */
  void parallel_read(System &sys);

  /** @return true if tasks are still executing. */
  [[nodiscard]] bool is_active() const {
    return m_n_completed.load(std::memory_order_relaxed) < m_ctx_id.load(std::memory_order_relaxed);
  }

 private:
  // clang-format off
  using Ctxs = std::list<std::shared_ptr<Ctx>>;

  using Scan_ctxs = std::list<std::shared_ptr<ScanCtx>>;

  // clang-format on

  /** Maximum number of worker threads to use. */
  size_t m_max_threads{};

  /** Number of worker threads that will be spawned. */
  size_t m_n_threads{0};

  /** Mutex protecting m_ctxs. */
  mutable Mutex m_mutex;

  /** Contexts that must be executed. */
  Ctxs m_ctxs{};

  /** Scan contexts. */
  Scan_ctxs m_scan_ctxs{};

  /** For signalling worker threads about events. */
  CondVar *m_event{};

  /** Value returned by previous call of os_event_reset() on m_event. */
  uint64_t m_sig_count;

  /** Counter for allocating scan context IDs. */
  size_t m_scan_ctx_id{};

  /** Context ID. Monotonically increasing ID. */
  std::atomic_size_t m_ctx_id{};

  /** Total tasks executed so far. */
  std::atomic_size_t m_n_completed{};

  /** Callback at start (before processing any rows). */
  Start m_start_callback{};

  /** Callback at end (adter processing all rows). */
  Finish m_finish_callback{};

  /** Error during parallel read. */
  std::atomic<DbErr> m_err{DB_SUCCESS};

  /** List of threads used for paralle_read purpose. */
  std::vector<std::thread> m_parallel_read_threads;


  /** If the caller wants to wait for the parallel_read to finish it's run */
  bool m_sync;

  /** Context information related to each parallel reader thread. */
  std::vector<ThreadCtx *> m_thread_ctxs;
};

/** Parallel reader context. */
class ParallelReader::ScanCtx {
 public:
  /** Constructor.
  @param[in]  reader          Parallel reader that owns this context.
  @param[in]  id              ID of this scan context.
  @param[in]  trx             Transaction covering the scan.
  @param[in]  config          Range scan config.
  @param[in]  f               Callback function. */
  ScanCtx(ParallelReader *reader, size_t id, Trx *trx, const ParallelReader::Config &config, F &&f);

  ~ScanCtx() noexcept;

 private:
  /** A part of the range: [first, second) by key, nullptr for no bound. */
  using Range = std::pair<const DTuple *, const DTuple *>;

  [[nodiscard]] size_t id() const { return m_id; }

  void set_error_state(DbErr err) { m_err.store(err, std::memory_order_relaxed); }

  [[nodiscard]] bool is_error_set() const { return m_err.load(std::memory_order_relaxed) != DB_SUCCESS; }

  /** Split the range of the scan in parts, by the keys of the index, and queue a context for each.
  @return DB_SUCCESS or error code. */
  [[nodiscard]] DbErr partition(System &sys);

  /** @param[in,out] rec  Current row read from the index: an older version if one has to be built.
  @param[in,out]  offsets       Same as above for the offsets.
  @param[in,out]  heap          For a previous version built from the undo log.
  @param[in,out]  mtr           Mini-transaction covering the read.
  @return true if row is visible to the transaction. */
  [[nodiscard]] bool check_visibility(System &sys, Rec &rec, RecOffs &offsets, MemHeap *&heap, Mtr *mtr);

  /** @return true if the secondary index record rec is visible to the read view of the scan. */
  [[nodiscard]] bool sec_rec_visible(System &sys, const Rec rec, MemHeap *&heap, Mtr *mtr);

  /** Queue an execution context for range.
  @return DB_SUCCESS or error code. */
  [[nodiscard]] DbErr create_context(const Range &range);

  [[nodiscard]] size_t max_threads() const { return m_reader->max_threads(); }

  using Config = ParallelReader::Config;

  /** Context ID. */
  size_t m_id{std::numeric_limits<size_t>::max()};

  /** Parallel scan configuration. */
  Config m_config;

  /** Covering transaction. */
  const Trx *m_trx{};

  /** Callback function. */
  F m_f;

  /** The parallel reader. */
  ParallelReader *m_reader{};

  /** The split keys, the bounds of the parts. */
  MemHeap *m_heap{};

  /** Error during parallel read. */
  mutable std::atomic<DbErr> m_err{DB_SUCCESS};

  friend class ParallelReader;

  ScanCtx(ScanCtx &&) = delete;
  ScanCtx(const ScanCtx &) = delete;
  ScanCtx &operator=(ScanCtx &&) = delete;
  ScanCtx &operator=(const ScanCtx &) = delete;
};

/** Parallel reader execution context: one part of the range of a scan. */
class ParallelReader::Ctx {
 public:
  /** Constructor.
  @param[in]    id              Thread ID.
  @param[in]    scan_ctx        Scan context.
  @param[in]    range           Range that the thread has to read. */
  Ctx(size_t id, ScanCtx *scan_ctx, const ScanCtx::Range &range) : m_id(id), m_range(range), m_scan_ctx(scan_ctx) {}

  ~Ctx() = default;

  /** @return the context ID. */
  [[nodiscard]] size_t id() const { return m_id; }

  /** The scan ID of the scan context this belongs to. */
  [[nodiscard]] size_t scan_id() const { return m_scan_ctx->id(); }

  /** @return the covering transaction. */
  [[nodiscard]] const Trx *trx() const { return m_scan_ctx->m_trx; }

  /** @return the index being scanned. */
  [[nodiscard]] const Index *index() const { return m_scan_ctx->m_config.m_index; }

  /** @return ID of the thread processing this context */
  [[nodiscard]] size_t thread_id() const { return m_thread_ctx->m_thread_id; }

  /** @return the thread context of the reader thread. */
  [[nodiscard]] ThreadCtx *thread_ctx() const { return m_thread_ctx; }

  /** @return the partition id of the index, std::numeric_limits<size_t>::max() if the index does not
  belong to a partition. */
  [[nodiscard]] size_t partition_id() const { return m_scan_ctx->m_config.m_partition_id; }

  /** Build an old version of the row if required.
  @param[in,out]  rec           Current row read from the index. This can
                                be modified by this method if an older version
                                needs to be built.
  @param[in,out]  offsets       Same as above but pertains to the rec offsets
  @param[in,out]  heap          Heap to use if a previous version needs to be
                                built from the undo log.
  @param[in,out]  mtr           Mini-transaction covering the read.
  @return true if row is visible to the transaction. */
  bool is_rec_visible(System &sys, Rec &rec, ulint *&offsets, MemHeap *&heap, Mtr *mtr) {
    RecOffs rec_offsets(offsets);
    return m_scan_ctx->check_visibility(sys, rec, rec_offsets, heap, mtr);
  }

 private:
  /** Read the range, calling the callback for each visible record.
  @return DB_SUCCESS or error code. */
  [[nodiscard]] DbErr traverse(System &sys);

  [[nodiscard]] bool is_error_set() const { return m_scan_ctx->m_reader->is_error_set() || m_scan_ctx->is_error_set(); }

  /** Context ID. */
  size_t m_id{std::numeric_limits<size_t>::max()};

  /** The range to scan. */
  ScanCtx::Range m_range{};

  /** The context of the scan. */
  ScanCtx *m_scan_ctx{};

 public:
  /** Context information related to executing thread ID. */
  ThreadCtx *m_thread_ctx{};

  /** Current row. */
  Rec m_rec{};

  /** The number of batches the range was read in so far. */
  size_t m_n_batches{};

  /** True if m_rec is the first record of a batch. */
  bool m_first_rec{true};

  /** Current row offsets. */
  RecOffs m_offsets{};

  /** Start of a new range to scan. */
  bool m_start{};

  friend class ParallelReader;
};
