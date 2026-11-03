/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

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

/** @file include/trx0sys.h
Transaction system

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "srv0sys.h"

#include "innodb0types.h"

#include "buf0buf.h"
#include "data0type.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "mem0mem.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "page0types.h"
#include "read0types.h"
#include "srv0srv.h"
#include "sync0sync.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0types.h"
#include "ut0byte.h"

#include <array>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "ut0lst.h"

/* The typedef for rseg slot in the file copy */
using trx_sysf_rseg_t = byte;

/** The automatically created system rollback segment has this id */
constexpr ulint TRX_SYS_SYSTEM_RSEG_ID = 0;

/** The transaction system tablespace.
Space id and page no where the trx system file copy resides */
constexpr auto TRX_SYS_SPACE = SYS_TABLESPACE;

/** Page numnber of the transaction system meta data. */
constexpr auto TRX_SYS_PAGE_NO = FSP_TRX_SYS_PAGE_NO;

/** The offset of the transaction system header on the page */
constexpr auto TRX_SYS = FSEG_PAGE_DATA;

/** Transaction system header */
/*@{ */

/** The maximum trx id or trx number modulo TRX_SYS_TRX_ID_UPDATE_MARGIN
written to a file page by any transaction; the assignment of transaction
ids continues from this number rounded up by TRX_SYS_TRX_ID_UPDATE_MARGIN
plus TRX_SYS_TRX_ID_UPDATE_MARGIN when the database is started */
constexpr ulint TRX_SYS_TRX_ID_STORE = 0;

/** Segment header for the tablespace segment the trx system is created into */
constexpr ulint TRX_SYS_FSEG_HEADER = 8;

/** The start of the array of rollback segment specification slots */
constexpr ulint TRX_SYS_RSEGS = 8 + FSEG_HEADER_SIZE;

/*@} */

/** Maximum number of rollback segments: the number of segment
specification slots in the transaction system array; rollback segment
id must fit in one byte, therefore 256; each slot is currently 8 bytes
in size */
constexpr ulint TRX_SYS_N_RSEGS = 256;

static_assert(UNIV_PAGE_SIZE >= 4096, "error UNIV_PAGE_SIZE < 4096");

/* Rollback segment specification slot offsets */
/*-------------------------------------------------------------*/

/** Tablespace where the segment header is placed. */
constexpr auto TRX_SYS_RSEG_SPACE = SYS_TABLESPACE;

/** Page number where the segment header is placed; this is FIL_NULL if the slot is unused */
constexpr page_no_t TRX_SYS_RSEG_PAGE_NO = 4;

/*-------------------------------------------------------------*/

/* Size of a rollback segment specification slot */
constexpr ulint TRX_SYS_RSEG_SLOT_SIZE = 8;

/** When a trx id which is zero modulo this number (which must be a power of
two) is assigned, the field TRX_SYS_TRX_ID_STORE on the transaction system
page is updated */
constexpr ulint TRX_SYS_TRX_ID_WRITE_MARGIN = 256;

/** The transaction system central memory data structure; protected by the
kernel mutex */
struct TrxSys {
  /** The instance this sub-system belongs to. */
  System &m_sys;


  /**
   * Constructor
   *
   * @param[in] fsp               File space management instance
   */
  explicit TrxSys(System &sys) noexcept;

  /**
   * Destructor
   */
  ~TrxSys() noexcept;

  /**
   * Create an instance of the transaction system.
   *
   * @param[in] fsp             File management instance to use
   *
   * @return new TrxSys instance.
   */
  /**
   * Destroy a transaction system instance.
   *
   * @param[in] recovery        Recovery flag
   */
  /** Start the transaction system.
   *
   * @return DB_SUCCESS or error code.
  */
  [[nodiscard]] DbErr start(IbRecovery recovery) noexcept;

  /**
   * Create a new system tablespace.
   *
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] DbErr create_system_tablespace() noexcept;

  /**
   * Open an existing database instance.
   *
   * @return DB_SUCCESS or error code.
   */
  [[nodiscard]] DbErr open_system_tablespace() noexcept;

  /**
   * Looks for a free slot for a rollback segment in the trx system file copy.
   *
   * @param[in] mtr               Mini-transaction handle
   *
   * @return	slot index or ULINT_UNDEFINED if not found
   */
  [[nodiscard]] ulint frseg_find_free(Mtr *mtr) noexcept;

  /**
   * Checks that trx is in the trx list.
   *
   * @return	true if is in
   */
  [[nodiscard]] bool in_trx_list(Trx *in_trx) noexcept;

  /**
   * Writes the value of max_trx_id to the file based trx system header.
   */
  void flush_max_trx_id() noexcept;

  /**
   * Looks for the trx handle with the given id in trx_list.
   *
   * @param[in] trx_id	Trx id to search for
   *
   * @return	the trx handle or NULL if not found
   */
  [[nodiscard]] Trx *get_on_id(trx_id_t trx_id) noexcept {
    ut_ad(mutex_own(&m_mutex));

    for (const auto trx : m_trx_list) {
      if (trx_id == trx->m_id) {

        return trx;
      }
    }

    return nullptr;
  }

  /**
   * Returns the minumum trx id in trx list. This is the smallest id for which
   * the trx can possibly be active. (But, you must look at the trx->conc_state to
   * find out if the minimum trx id transaction itself is active, or already
   * committed.)
   *
   * @return	the minimum trx id, or TrxSys::max_trx_id if the trx list is empty
   */
  [[nodiscard]] trx_id_t get_min_trx_id() const noexcept {
    ut_ad(mutex_own(&m_mutex));

    auto trx = UT_LIST_GET_LAST(m_trx_list);

    return trx == nullptr ? m_max_trx_id : trx->m_id;
  }

  /**
   * Checks if a transaction with the given id is active.
   *
   * @param[in] trx_id	Trx id of the transaction
   *
   * @return	true if active
   */
  [[nodiscard]] bool is_active(trx_id_t trx_id) noexcept {
    ut_ad(mutex_own(&m_mutex));

    if (trx_id < get_min_trx_id()) {

      return false;
    }

    if (trx_id >= m_max_trx_id) {

      /* There must be corruption: we return true because this
      function is only called by lock_clust_rec_some_has_impl()
      and row_vers_impl_x_locked_off_kernel() and they have
      diagnostic prints in this case */

      return true;
    } else {
      auto trx = get_on_id(trx_id);

      return trx != nullptr && (trx->m_conc_state == TRX_ACTIVE || trx->m_conc_state == TRX_PREPARED);
    }
  }

  /**
   * Allocates a new transaction id.
   *
   * @return	new, allocated trx id
   */
  [[nodiscard]] trx_id_t get_new_trx_id() noexcept {
    ut_ad(mutex_own(&m_mutex));

    /* VERY important: after the database is started, max_trx_id value is
    divisible by TRX_SYS_TRX_ID_WRITE_MARGIN, and the following if
    will evaluate to true when this function is first time called,
    and the value for trx id will be written to disk-based header!
    Thus trx id values will not overlap when the database is
    repeatedly started! */

    if (m_max_trx_id % TRX_SYS_TRX_ID_WRITE_MARGIN == 0) {

      flush_max_trx_id();
    }

    auto id = m_max_trx_id;

    ++m_max_trx_id;

    return id;
  }

  /**
   * Allocates a new transaction number.
   *
   * @return	new, allocated trx number
   */
  [[nodiscard]] trx_id_t get_new_trx_no() noexcept {
    ut_ad(mutex_own(&m_mutex));

    return get_new_trx_id();
  }

  /**
     * This function is used to find number of prepared transactions and
     * their transaction objects for a recovery. This function is used to
     * recover any X/Open XA distributed transactions.
     *
     * @param[in,out] xid_list Prepared transactions.
     * @param[in] len Number of slots in xid_list.
     *
     * @return Number of prepared transactions.
     */
  [[nodiscard]] int recover(Xid *xid_list, ulint len) noexcept;

  /**
   * Gets a pointer to the transaction system header and x-latches its page.
   *
   * @param[in]	mtr	mtr
   *
   * @return	pointer to system header, page x-latched.
   */
  [[nodiscard]] trx_sysf_t *read_header(Mtr *mtr) noexcept {
    ut_ad(mtr != nullptr);

    BufPool::Request req{
      .m_rw_latch = RW_X_LATCH,
      .m_page_id = {TRX_SYS_SPACE, TRX_SYS_PAGE_NO},
      .m_mode = BUF_GET,
      .m_file = __FILE__,
      .m_line = __LINE__,
      .m_mtr = mtr
    };

    auto block = m_sys.m_buf_pool->get(req, nullptr);
    buf_block_dbg_add_level();

    return TRX_SYS + block->get_frame();
  }

  /** Gets the pointer in the nth slot of the rseg array.
   *
   * @param[in] sys	Trx system
   * @param[in] n	Index of slot
   *
   * @return	pointer to rseg object, NULL if slot not in use
   */
  [[nodiscard]] TrxRseg *get_nth_rseg(ulint n) noexcept {
    ut_ad(mutex_own(&m_mutex));
    ut_ad(n < m_rsegs.size());

    return m_rsegs[n];
  }

  /**
   * Sets the pointer in the nth slot of the rseg array.
   *
   * @param[in] sys	Trx system
   * @param[in] n	Index of slot
   * @param[in] rseg	Pointer to rseg object, NULL if slot not in use
   */
  void set_nth_rseg(ulint n, TrxRseg *rseg) noexcept {
    ut_ad(n < m_rsegs.size());

    m_rsegs[n] = rseg;
  }

  /**
   * @brief Inserts the trx handle in the trx system trx list in the right position.
   *
   * The list is sorted on the trx id so that the biggest id is at the list start.
   * This function is used at the database startup to insert incomplete transactions to the list.
   *
   * @param[in]  trx The transaction handle to insert.
   */
  void trx_list_insert_ordered(Trx *trx) noexcept;

  /**
   * Creates trx objects for transactions and initializes the trx list of this at database start.
   * Rollback segment and undo log lists must already exist when this function is called,
   * because the lists of transactions to be rolled back or cleaned up are built based on the undo log lists.
   *
   * @param[in] recovery The recovery flag.
   */
  void trx_lists_init_at_db_start(IbRecovery recovery) noexcept;

  /**
   * Assigns a rollback segment to a transaction in a round-robin fashion.
   * Skips the SYSTEM rollback segment if another is available.
   *
   * @return	assigned rollback segment id
   */
  [[nodiscard]] ulint trx_assign_rseg() noexcept;

  /**
   * Creates a background transaction instance.
   *
   * @param[in] arg	Any context that needs to be passed to the trx.
   *
   * @return own: transaction object
   */
  [[nodiscard]] Trx *create_background_trx(void *arg) noexcept;

  /**
   * Frees a background transaction instance.
   *
   * @param[in] trx The transaction object to be freed.
   */
  void destroy_background_trx(Trx *&trx) noexcept;

  /**
   * Creates a user transaction instance.
   *
   * @param[in] arg	Any context that needs to be passed to the trx.
   *
   * @return own: transaction object
   */
  [[nodiscard]] Trx *create_user_trx(void *arg) noexcept;

  /**
   * Frees a client transaction instance.
   *
   * @param[in] trx The transaction object to be freed.
   */
  void destroy_user_trx(Trx *&trx) noexcept;

  /**
   * Checks if a page address is the trx sys header page.
   *
   * @param[in] space	Tablespace ID
   * @param[in] page_no	Page number
   *
   * @return	true if trx sys header page
   */
  [[nodiscard]] static bool is_hdr_page(space_id_t space_id, page_no_t page_no) noexcept {
    return space_id == TRX_SYS_SPACE && page_no == TRX_SYS_PAGE_NO;
  }

  /** Gets the space of the nth rollback segment slot in the trx system file copy.
   *
   * @param[in]	sys_header	Trx sys header
   * @param[in]	i	Slot index == rseg id
   * @param[in]	mtr	mtr
   *
   * @return	space id
   */
  [[nodiscard]] static space_id_t frseg_get_space(trx_sysf_t *sys_header, ulint i, Mtr *mtr) noexcept {
    ut_ad(i < TRX_SYS_N_RSEGS);
    // TODO: Remove this once we have a proper mutex for the trx system.
    // ut_ad(mutex_own(&m_mutex));

    return mtr->read_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE, MLOG_4BYTES);
  }

  /**
   * Gets the page number of the nth rollback segment slot in the trx system header.
   *
   * @param[in]	sys_header	Trx sys header
   * @param[in]	i	Slot index == rseg id
   * @param[in]	mtr	mtr
   *
   * @return	page number, FIL_NULL if slot unused
   */
  [[nodiscard]] static page_no_t frseg_get_page_no(trx_sysf_t *sys_header, ulint i, Mtr *mtr) noexcept {
    ut_ad(i < TRX_SYS_N_RSEGS);
    // TODO: Remove this once we have a proper mutex for the trx system.
    // ut_ad(mutex_own(&m_mutex));

    return mtr->read_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_PAGE_NO, MLOG_4BYTES);
  }

  /**
   * Sets the space id of the nth rollback segment slot in the trx system
   * file copy.
   *
   * @param[in]	sys_header	Trx sys file copy
   * @param[in]	i	Slot index == rseg id
   * @param[in]	space	Space id
   * @param[in]	mtr	mtr
   */
  static void frseg_set_space(trx_sysf_t *sys_header, ulint i, space_id_t space, Mtr *mtr) noexcept {
    ut_ad(i < TRX_SYS_N_RSEGS);
    // TODO: Remove this once we have a proper mutex for the trx system.
    // ut_ad(mutex_own(&m_mutex));

    mlog_write_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE, space, MLOG_4BYTES, mtr);
  }

  /**
   * Sets the page number of the nth rollback segment slot in the trx system header.
   *
   * @param[in]	sys_header	Trx sys header
   * @param[in]	i	Slot index == rseg id
   * @param[in]	page_no	Page number, FIL_NULL if the slot is reset to unused
   * @param[in]	mtr	mtr
   */

  static void frseg_set_page_no(trx_sysf_t *sys_header, ulint i, page_no_t page_no, Mtr *mtr) noexcept {
    ut_ad(i < TRX_SYS_N_RSEGS);
    // TODO: Remove this once we have a proper mutex for the trx system.
    // ut_ad(mutex_own(&m_mutex));

    mlog_write_ulint(sys_header + TRX_SYS_RSEGS + i * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_PAGE_NO, page_no, MLOG_4BYTES, mtr);
  }

  /**
   * Writes a trx id to an index page. In case that the id size changes in
   * some future version, this function should be used instead of
   * mach_write_...
   *
   * @param[in]	ptr	Pointer to memory where written
   * @param[in]	id	transaction id to write
  */
  static void write_trx_id(byte *ptr, trx_id_t id) noexcept {
    static_assert(DATA_TRX_ID_LEN == 6, "error DATA_TRX_ID_LEN != 6");

    mach_write_to_6(ptr, id);
  }

  /**
   * Reads a trx id from an index page. In case that the id size changes in
   * some future version, this function should be used instead of
   * mach_read_...
   *
   * @param[in]	ptr	Pointer to memory from where to rckgrouneadestroy_background_trxd
   *
   * @return	id
   * */
  [[nodiscard]] static trx_id_t read_trx_id(const byte *ptr) noexcept {
    static_assert(DATA_TRX_ID_LEN == 6, "error DATA_TRX_ID_LEN != 6");

    return mach_read_from_6(ptr);
  }

#ifdef WITH_XOPEN
  /**
   * This function is used to find one X/Open XA distributed transaction
   * which is in the prepared state
   *
   * @param[in] xid X/Open XA transaction identification
   *
   * @return trx or nullptr
   */
  [[nodiscard]] Trx *get_trx_by_xid(Xid *xid) noexcept;
#endif /* WITH_XOPEN */

  /** Closes a read view.
   * @param view read view
   */
  void close_read_view(ReadView *view);

  /** Closes a consistent read view for client. This function is called at an SQL
   * statement end if the trx isolation level is <= TRX_ISO_READ_COMMITTED.
   * @param trx trx which has a read view
   */
  void close_read_view_for_read_committed(Trx *trx);

  /**
   * This function sets a given consistent cursor view to a transaction
   * read view if given consistent cursor view is not nullptr. Otherwise, function
   * restores a global read view to a transaction read view.
   *
   * @param trx in: transaction where cursor is set
   * @param cursor_view in: consistent cursor view to be set
   */
  void set_cursor_view(Trx *trx, CursorView *cursor_view);

  /** Create a consistent cursor view to be used in cursors. In this
   * consistent read view modifications done by the creating transaction or future
   * transactions are not visible.
   * @param cr_trx trx where cursor view is created
   * @return cursor view
   */
  CursorView *create_cursor_view(Trx *cr_trx);

  /** Close a given consistent cursor view and restore global read view
   * back to a transaction read view.
   * @param trx trx
   * @param curview cursor view to be closed
   */
  void close_cursor_view(Trx *trx, CursorView *curview);

  /** Creates a read view object.
  @param[in] n                    Number of cells in the trx_ids array.
  @param[in,out] heap             Memory heap to use for allocation.
  @return	own: read view struct */
  ReadView *create_read_view_low(ulint n, MemHeap *heap);

  /**
   * Makes a copy of the oldest existing read view, or opens a new. The view
   * must be closed with ..._close.
   *
   * @param cr_trx_id trx_id of creating transaction, or 0 used in purge
   * @param heap memory heap from which allocated
   * @return own: read view struct
   */
  ReadView *oldest_copy_or_open_new(trx_id_t cr_trx_id, MemHeap *heap);

  /**
   * Opens a read view where exactly the transactions serialized before this
   * point in time are seen in the view.
   *
   * @param cr_trx_id trx_id of creating transaction, or 0 used in purge
   * @param heap memory heap from which allocated
   * @return own: read view struct
   */
  ReadView *open_read_view_now(trx_id_t cr_trx_id, MemHeap *heap);

#ifdef UNIT_TEST
 private:
#endif /* UNIT_TEST */

  /**
   * Creates the file page for the transaction system. This function is called
   * only at the database creation, before init().
   *
   * @param[in,out] mtr              Mini-transaction covering the operation.
   */
  void create_new_instance(Mtr *mtr) noexcept;

  /**
   * Creates trx objects for transactions and initializes the trx list of this at database start.
   * Rollback segment and undo log lists must already exist when this function is called,
   * because the lists of transactions to be rolled back or cleaned up are built based on the undo log lists.
   *
   * @param[in] recovery The recovery flag.
   */
  void init_at_db_start(IbRecovery recovery) noexcept;

  struct IdShard;

  /** @return the shard of the active ids of an id. */
  [[nodiscard]] IdShard &id_shard(trx_id_t id) noexcept { return m_ids[(id * 0x9E3779B97F4A7C15ULL) >> 56]; }

  /** Insert trx in the active ids. */
  void ids_insert(Trx *trx) noexcept;

  /** Erase trx from the active ids, if it is there (C2 of its commit). */
  void ids_erase(Trx *trx) noexcept;

  /**
   * Finds an active transaction by its id and pins it: it can't reach step
   * C4 of its commit (committed in memory) until unpin(), nor can the Trx
   * object be reused.
   *
   * @param[in] id A transaction id.
   *
   * @return the pinned transaction, or nullptr if no active one has the id.
   */
  [[nodiscard]] Trx *pin_active(trx_id_t id) noexcept;

  /** Unpin a transaction pinned by pin_active(). */
  static void unpin(Trx *trx) noexcept { trx->m_n_ref.fetch_sub(1, std::memory_order_release); }

  /**
   * @return true if a transaction with the id is in the active ids (not past
   * C2 of its commit), read with the id shard mutex. V3 of a conversion.
   */
  [[nodiscard]] bool in_ids(trx_id_t id) noexcept;

  /**
   * Waits until no thread pins trx (step C3 of its commit). The calling
   * thread holds no latch: a pinning thread may wait for a page latch or a
   * lock system latch while it holds the pin.
   *
   * @param[in] trx A transaction erased from the active ids.
   */
  void drain_pins(Trx *trx) noexcept;

  /** Set m_min_active_id from the list of transactions; the trx sys mutex is held. */
  void update_min_active_id() noexcept { m_min_active_id.store(get_min_trx_id(), std::memory_order_release); }

  /**
   * Creates a transaction instance.
   *
   * @param[in] arg Any context that needs to be passed to the trx.
   *
   * @return own: transaction object
   */
  [[nodiscard]] Trx *create_trx(void *arg) noexcept;

  /**
   * Destroys a transaction instance.
   *
   * @param[in] trx The transaction object to be freed.
   */
  void destroy_trx(Trx *&trx) noexcept;

 public:
  /** Currently serves the purpose of the old kernel mutex.
   * TODO: Split it over a few patches.
   */
  Mutex m_mutex{};

  /** The smallest number not yet assigned as a transaction
  id or transaction number */
  trx_id_t m_max_trx_id{};

  /** List of read views sorted on trx no, biggest first */
  UT_LIST_BASE_NODE_T_EXTERN(ReadView, m_view_list) m_view_list {};

  /** List of active and committed in memory transactions,
  sorted on trx id, biggest first */
  UT_LIST_BASE_NODE_T_EXTERN(Trx, m_trx_list) m_trx_list {};

  /** List of transactions created for users */
  UT_LIST_BASE_NODE_T_EXTERN(Trx, m_client_trx_list) m_client_trx_list {};

  /** List of rollback segment objects */
  UT_LIST_BASE_NODE_T_EXTERN(TrxRseg, m_rseg_list) m_rseg_list {};

  /* Active transactions by id, for the implicit locks (the lock system
  finds the owner of a record's trx id without the trx sys mutex; see the
  LATCHING comment of lock0lock.h). A transaction is in it from the
  assignment of its id (start, or recovery for an active or prepared one)
  to step C2 of its commit. A shard mutex is below the trx sys mutex
  (SYNC_TRX_SYS_SHARD) and above the trx mutex. */

  /** Number of shards of the active ids. */
  static constexpr size_t N_ID_SHARDS = 256;

  /** A shard of the active ids. */
  struct alignas(64) IdShard {
    std::mutex m_mutex;
    std::unordered_map<trx_id_t, Trx *> m_trxs;
  };

  /** The shards. */
  std::array<IdShard, N_ID_SHARDS> m_ids{};

  /** A lower bound of the smallest id of an active transaction (or
  m_max_trx_id if none): updated with the trx sys mutex when the list of
  transactions changes, read without it. A stale value is smaller, which
  only makes a reader look for implicit locks it needn't. */
  std::atomic<trx_id_t> m_min_active_id{};

  /** The memory of destroyed Trx objects, reused by create_trx() and freed
  with the TrxSys: a Trx is type stable while the System lives, the deadlock
  walk can read the edge of a transaction that committed under it (see
  lock0deadlock.h). */
  std::vector<void *> m_trx_pool{};

  /** Protects m_trx_pool. */
  std::mutex m_trx_pool_mutex{};

  /** Latest rollback segment in the round-robin assignment
  of rollback segments to transactions */
  TrxRseg *m_latest_rseg{};

  /** Pointer array to rollback segments; NULL if slot not in use */
  std::array<TrxRseg *, TRX_SYS_N_RSEGS> m_rsegs{};

  /** Length of the TRX_RSEG_HISTORY list (update undo logs for
  committed transactions), protected by rseg->mutex */
  ulint m_rseg_history_len{};

  /** The following is true when we are using the database in the file per table
   * format, we have successfully upgraded, or have created a new database installation */
  bool m_multiple_tablespace_format{};

  /** Number of transactions currently allocated for the client: protected by
  the kernel mutex */
  ulint m_n_user_trx{};

  /** Number of background transactions currently allocated: protected by
   * the kernel mutex */
  ulint m_n_background_trx{};


  /** Purge system. */
  PurgeSys *m_purge{};

  /** The recovered transaction being rolled back in crash recovery, for
  reporting the rollback progress. */
  Trx *m_recv_rollback_trx{};

  /** Undo number of m_recv_rollback_trx when its rollback started. */
  int64_t m_recv_rollback_max_undo_no{};

  /** Last rollback progress percentage reported. */
  ulint m_recv_rollback_printed_pct{};
};

