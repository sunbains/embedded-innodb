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

/** @file trx/trx0sys.c
Transaction system

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#include "trx0sys.h"
#include "buf0dblwr.h"

#include <algorithm>
#include <chrono>
#include <thread>
#include "fsp0fsp.h"
#include "log0log.h"
#include "mtr0log.h"
#include "os0file.h"
#include "read0read.h"
#include "srv0srv.h"
#include "trx0purge.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0undo.h"


TrxSys::TrxSys(System &sys) noexcept : m_sys(sys) {
  mutex_create(&m_mutex, IF_DEBUG("trx_sys_mutex", )  Current_location());
}

TrxSys::~TrxSys() noexcept {
  /* Check that all read views are closed. Purge's holds no slot. */

  if (const auto n = n_read_views(); n > 0) {
    log_err(std::format("All read views were not closed before shutdown: {} read views open", n));
  }

  /* Purge is created late in startup: an aborted startup has none. */
  if (m_purge != nullptr) {
    auto purge_trx = m_purge->m_trx;

    call_destructor(m_purge);
    ut_delete(m_purge);

    destroy_background_trx(purge_trx);
  }

  /* This is required only because it's a pre-condition for many
  of the functions that we need to call. */
  mutex_enter(&m_mutex);

  /* There can't be any active transactions. */
  auto rseg = m_rseg_list.front();

  while (rseg != nullptr) {
    auto prev_rseg = rseg;

    rseg = UT_LIST_GET_NEXT(m_rseg_list, prev_rseg);

    m_rseg_list.remove(prev_rseg);

    prev_rseg->memory_free(m_sys);
  }

  ut_a(active_ids().empty());
  ut_a(m_recv_trx_list.empty());
  ut_a(m_rseg_list.empty());

  for (auto &shard : m_client_shards) {
    ut_a(shard.m_trxs.empty());
  }

  mutex_exit(&m_mutex);

  mutex_free(&m_mutex);

  for (auto &[mem, event] : m_trx_pool) {
    os_event_free(event);
    mem_free(mem);
  }
}

void TrxSys::ids_insert(trx_id_t id, TrxNode *node) noexcept {
  ut_ad(id != 0);

  auto &shard = id_shard(id);
  std::lock_guard<ut::Latch> guard(shard.m_mutex);

  shard.m_trxs.insert(id, node);
}

void TrxSys::ids_erase(Trx *trx) noexcept {
  if (trx->m_id == 0 || trx->m_node == nullptr) {
    return;
  }

  auto &shard = id_shard(trx->m_id);
  std::lock_guard<ut::Latch> guard(shard.m_mutex);

  static_cast<TrxNode *>(trx->m_node)->m_payload.m_trx.store(nullptr, std::memory_order_release);
}

Trx *TrxSys::pin_active(trx_id_t id) noexcept {
  auto &shard = id_shard(id);
  std::lock_guard<ut::Latch> guard(shard.m_mutex);

  if (auto node = shard.m_trxs.find(id); node != nullptr) {
    if (auto trx = node->m_payload.m_trx.load(std::memory_order_acquire); trx != nullptr) {
      /* Active or prepared (it stops being it before it is committed in memory).
      The pin is taken with the shard latch, which the C2 takes too: after C2, no
      new pin. An id of another node is not in the table. */
      trx->m_n_ref.fetch_add(1, std::memory_order_acquire);

      return trx;
    }
  }

  return nullptr;
}

bool TrxSys::in_ids(trx_id_t id) noexcept {
  auto &shard = id_shard(id);
  std::lock_guard<ut::Latch> guard(shard.m_mutex);

  const auto node = shard.m_trxs.find(id);

  return node != nullptr && node->m_payload.m_trx.load(std::memory_order_acquire) != nullptr;
}

bool TrxSys::visible_at(trx_id_t id, uint64_t snap) noexcept {
  auto &shard = id_shard(id);
  std::lock_guard<ut::Latch> guard(shard.m_mutex);

  const auto node = shard.m_trxs.find(id);

  if (node == nullptr) {
    return true;
  }

  const auto c = m_nodes.final_seq(*node);

  return c != 0 && c <= snap;
}

bool ReadView::lookup(trx_id_t id) const noexcept { return m_trx_sys->visible_at(id, m_snap_word); }

void TrxSys::node_start(Trx *trx) noexcept {
  TrxNode *node;

  for (ulint i{}; (node = m_nodes.alloc()) == nullptr; ++i) {
    /* The limit of the nodes: an old read view holds the horizon. */
    retire();

    if (i % 10000 == 0) {
      log_warn("The commit table is full, a transaction waits: close the oldest read view");
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  node->m_payload.m_no.store(LSN_MAX, std::memory_order_relaxed);
  node->m_payload.m_trx.store(trx, std::memory_order_relaxed);

  /* In the list first: an id <= a word read by the retirer is of a node its walk
  can meet (READ VIEWS, W). */
  m_nodes.push(node);

  const auto id = m_nodes.final_ticket(*node);

  hlc_issued(ut::id_to_hlc(id));

  trx->m_id = id;
  trx->m_node = node;

  /* Before T writes a row with the id. */
  ids_insert(id, node);
}

uint64_t TrxSys::commit_stamp(Trx *trx, uint64_t wall) noexcept {
  auto node = static_cast<TrxNode *>(trx->m_node);

  if (m_commit_hook != nullptr) {
    m_commit_hook(m_commit_hook_arg, trx, 0);
  }

  m_nodes.mark_pending(node);

  if (m_commit_hook != nullptr) {
    m_commit_hook(m_commit_hook_arg, trx, 1);
  }

  const auto mine = next_word(wall);

  if (m_commit_hook != nullptr) {
    m_commit_hook(m_commit_hook_arg, trx, 2);
  }

  /* A helper's may have won: above mine, or below. */
  auto stamp = TrxList::PENDING;

  if (node->m_seq.compare_exchange_strong(stamp, mine, std::memory_order_seq_cst, std::memory_order_seq_cst)) {
    stamp = mine;
  }

  hlc_issued(ut::id_to_hlc(stamp));

  if (m_commit_hook != nullptr) {
    m_commit_hook(m_commit_hook_arg, trx, 3);
  }

  /* The list's now: it can't be touched by this thread any more. */
  trx->m_node = nullptr;
  TrxList::release(node);

  if (m_nodes.sweep_due(stamp)) {
    retire();
  }

  return stamp;
}

void TrxSys::trx_list_remove(Trx *trx) noexcept {
  ids_erase(trx);

  if (auto node = static_cast<TrxNode *>(trx->m_node); node != nullptr) {
    (void)m_nodes.remove(node);
    trx->m_node = nullptr;
  }

  recv_list_remove(trx);
}

void TrxSys::recv_list_remove(Trx *trx) noexcept {
  if (trx->m_in_recv_list) {
    mutex_enter(&m_mutex);

    m_recv_trx_list.remove(trx);
    trx->m_in_recv_list = false;

    mutex_exit(&m_mutex);
  }
}

void TrxSys::retire() noexcept {
  /* The order of reads is the proof of READ VIEWS: the clock, the slots. */
  const auto c0 = m_clock.read_word();
  const auto h = m_snaps.min(c0);

  auto w = ut::word_id_limit(c0);

  const auto visit = [&](TrxNode &node) {
    const auto id = m_nodes.final_ticket(node);
    const auto c = m_nodes.final_seq(node);

    if ((c == 0 || c > c0) && id < w) {
      w = id;
    }
  };

  const auto unlink = [&](TrxNode &node) {
    const auto id = node.m_ticket.load(std::memory_order_seq_cst);

    auto &shard = id_shard(id);
    std::lock_guard<ut::Latch> guard(shard.m_mutex);

    shard.m_trxs.erase(id, &node);
  };

  if (m_nodes.try_sweep(h, visit, unlink)) {
    raise(m_low_water, w);
    raise(m_snap_floor, h);
  }
}

void TrxSys::refresh_low_water() noexcept {
  m_low_water.store(get_min_trx_id(), std::memory_order_release);
}

trx_id_t TrxSys::get_min_trx_id() noexcept {
  const auto ids = active_ids();

  return ids.empty() ? max_trx_id() : ids.back();
}

std::vector<trx_id_t> TrxSys::active_ids() noexcept {
  std::vector<trx_id_t> ids;

  {
    TrxList::Scan scan(m_nodes);

    for (auto node = scan.head(); node != nullptr; node = TrxList::Scan::next(node)) {
      if (m_nodes.final_seq(*node) == 0) {
        ids.push_back(scan.ticket(*node));
      }
    }
  }

  /* A push before its id: the order may be off. */
  std::sort(ids.begin(), ids.end(), std::greater<>());

  return ids;
}

bool TrxSys::trx_list_empty() noexcept {
  ut_ad(mutex_own(&m_mutex));

  return active_ids().empty() && m_recv_trx_list.empty();
}

Trx *TrxSys::get_on_id(trx_id_t id) noexcept {
  {
    auto &shard = id_shard(id);
    std::lock_guard<ut::Latch> guard(shard.m_mutex);

    if (auto node = shard.m_trxs.find(id); node != nullptr) {
      if (auto trx = node->m_payload.m_trx.load(std::memory_order_acquire); trx != nullptr) {
        return trx;
      }
    }
  }

  /* A recovered transaction committed in memory has no node. */
  mutex_enter(&m_mutex);

  Trx *found{};

  for (auto trx : m_recv_trx_list) {
    if (trx->m_id == id) {
      found = trx;
      break;
    }
  }

  mutex_exit(&m_mutex);

  return found;
}

bool TrxSys::in_trx_list(Trx *trx) noexcept { return trx->m_node != nullptr || trx->m_in_recv_list; }

void TrxSys::drain_pins(Trx *trx) noexcept {
  ut_ad(TrxMutex::n_held() == 0);

  for (ulint i{}; trx->m_n_ref.load(std::memory_order_acquire) > 0; ++i) {
    /* A conversion holds it for a lock request: a short wait. */
    if (i < 64) {
      std::this_thread::yield();
    } else {
      std::this_thread::sleep_for(std::chrono::microseconds(i < 1024 ? 10 : 100));
    }

#ifdef UNIV_DEBUG
    if (i == 100000) {
      log_warn(std::format("trx {} waited long for the pins of lock conversions: {}", trx->m_id, trx->m_n_ref.load()));
    }
#endif /* UNIV_DEBUG */
  }
}

DbErr TrxSys::start(IbRecovery recovery) noexcept {
  const char *unit = "";

  auto purge_trx = create_background_trx(nullptr);

  Mtr mtr;

  mtr.start();

  mutex_enter(&m_mutex);

  auto sys_header = read_header(&mtr);

  TrxRseg::list_and_array_init(m_sys, recovery, sys_header, &mtr);

  m_latest_rseg = m_rseg_list.front();

  const auto stored = mtr.read_uint64(sys_header + TRX_SYS_HLC_STORE);

  m_clock.set_node(uint32_t(mach_read_from_4(sys_header + TRX_SYS_NODE)));

  init_at_db_start(recovery);

  /* The clock restarts past anything issued: above the store plus the margin
  (hlc_store()), which includes the ids and numbers of the undo logs, checked
  anyway, and the wall clock if it is ahead. It is seeded above the base: no id
  underflows, and none is 0, whatever the wall clock says. */
  {
    uint64_t last = std::max({stored + TRX_SYS_HLC_MARGIN_US, m_clock.wall(), ut::HLC_BASE_US + 1, m_sys.m_config.m_trx.m_hlc_start});
    uint64_t epoch = std::max(mtr.read_uint64(sys_header + TRX_SYS_EPOCH), m_sys.m_config.m_trx.m_epoch);

    const auto note = [&](trx_id_t id) {
      last = std::max(last, ut::id_to_hlc(id));
      epoch = std::max(epoch, ut::id_to_epoch(id));
    };

    for (auto trx : m_recv_trx_list) {
      note(trx->m_id);

      if (trx->m_no != LSN_MAX) {
        note(trx->m_no);
      }
    }

    for (auto rseg : m_rseg_list) {
      if (rseg->m_last_page_no != FIL_NULL) {
        note(rseg->m_last_trx_no);
      }
    }

    m_hlc_stored.store(stored, std::memory_order_release);
    m_clock.reset(epoch, last);

    /* The recovered ones are in the table: W can't be above them. */
    refresh_low_water();
    m_snap_floor.store(m_clock.read_word(), std::memory_order_release);
  }

  int64_t rows_to_undo{};

  if (!m_recv_trx_list.empty()) {
    for (auto trx : m_recv_trx_list) {
      if (trx->m_conc_state != TRX_PREPARED) {
        rows_to_undo += trx->m_undo_no;
      }
    }

    if (rows_to_undo > 1000000000) {
      unit = "M";
      rows_to_undo = rows_to_undo / 1000000;
    }

    log_info(std::format(
      "{} transaction(s) which must be rolled back or cleaned up"
      " in total {}s row operations to undo",
      m_recv_trx_list.size(),
      rows_to_undo,
      unit
    ));

    log_info(std::format("Trx id high water is {}", max_trx_id()));
  }

  {
    ut_a(m_purge == nullptr);

    auto ptr = ut_new(sizeof(PurgeSys));
    m_purge = new (ptr) PurgeSys(m_sys, purge_trx);
  }

  mutex_exit(&m_mutex);

  mtr.commit(m_sys);

  return DB_SUCCESS;
}

void TrxSys::hlc_store(uint64_t hlc) noexcept {
  /* The latch even for a thread that doesn't have to wait yet: the master thread
  only comes here while nobody is in a hurry. */
  m_hlc_store_latch.lock();

  if (hlc > m_hlc_stored.load(std::memory_order_relaxed)) {
    Mtr mtr;

    mtr.start();

    auto sys_header = read_header(&mtr);

    mlog_write_uint64(sys_header + TRX_SYS_HLC_STORE, hlc, &mtr);
    mlog_write_uint64(sys_header + TRX_SYS_EPOCH, m_clock.epoch(), &mtr);

    mtr.commit(m_sys);

    /* Durable before any thread relies on it: a commit is durable to its own
    lsn only, which may be before this. */
    m_sys.m_log->write_up_to(mtr.m_end_lsn, m_sys.m_config.m_fil.m_flush_method != SRV_UNIX_NOSYNC);

    m_hlc_stored.store(hlc, std::memory_order_release);
  }

  m_hlc_store_latch.unlock();
}

void TrxSys::clock_set_epoch(uint64_t epoch) noexcept {
  std::lock_guard<ut::Latch> guard(m_hlc_store_latch);

  /* Fatal past the last one. */
  if (epoch >= ut::HLC_MAX_EPOCHS) {
    m_clock.set_epoch(epoch);
  }

  /* Durable first: no id of the epoch before a restart knows of it. */
  Mtr mtr;

  mtr.start();

  mlog_write_uint64(read_header(&mtr) + TRX_SYS_EPOCH, epoch, &mtr);

  mtr.commit(m_sys);

  m_sys.m_log->write_up_to(mtr.m_end_lsn, m_sys.m_config.m_fil.m_flush_method != SRV_UNIX_NOSYNC);

  m_clock.set_epoch(epoch);
}

DbErr TrxSys::check_format(os_file_t fh, uint32_t node) noexcept {
  auto ptr = static_cast<byte *>(ut_new(2 * UNIV_PAGE_SIZE));
  auto page = static_cast<byte *>(ut_align(ptr, UNIV_PAGE_SIZE));

  auto success = os_file_read(fh, page, UNIV_PAGE_SIZE, off_t(TRX_SYS_PAGE_NO) * UNIV_PAGE_SIZE);
  ut_a(success);

  auto err{DB_SUCCESS};
  const auto hdr = page + TRX_SYS;

  /* All zero: the page was never written, a crash while the instance was being
  created. The redo log has it, and it is of this format: the log of an older
  format file has a written page here. */
  const bool empty = std::all_of(page, page + UNIV_PAGE_SIZE, [](byte b) { return b == 0; });

  if (empty) {
    /* Nothing to check. */
  } else if (mach_read_from_8(hdr + TRX_SYS_FORMAT_MAGIC) != TRX_SYS_FORMAT_MAGIC_N) {
    log_err(
      "The data files are of an older format, without clock based transaction ids. There is no upgrade path:"
      " this version can't read them. Create a new instance and load the data into it."
    );
    err = DB_ERROR;
  } else if (const auto version = mach_read_from_4(hdr + TRX_SYS_FORMAT_VERSION); version != TRX_SYS_FORMAT_VERSION_N) {
    log_err(std::format("The data files are of format version {}, this version reads format version {} only.", version, TRX_SYS_FORMAT_VERSION_N));
    err = DB_ERROR;
  } else if (const auto stored = mach_read_from_4(hdr + TRX_SYS_NODE); stored != node) {
    /* An id says nothing of its node, a row is of the instance: rebuilt
    timestamps of the history would change node, and two nodes would issue the
    same timestamps, if this were let pass. */
    log_err(std::format("The data files belong to node id {}, the configuration says node id {}. Set the node id of the data files.", stored, node));
    err = DB_ERROR;
  }

  ut_delete(ptr);

  return err;
}

ulint TrxSys::frseg_find_free(Mtr *mtr) noexcept {
  ut_ad(mutex_own(&m_mutex));

  auto sys_header = read_header(mtr);

  for (ulint i{}; i < TRX_SYS_N_RSEGS; ++i) {

    const auto page_no = frseg_get_page_no(sys_header, i, mtr);

    if (page_no == FIL_NULL) {

      return i;
    }
  }

  return ULINT_UNDEFINED;
}

DbErr TrxSys::create_system_tablespace() noexcept {
  Mtr mtr;

  mtr.start();

  /* Note that below we first reserve the file space x-latch, and
  then enter the kernel: we must do it in this order to conform
  to the latching order rules. */

  m_sys.m_fsp->latch(TRX_SYS_SPACE, &mtr);

  mutex_enter(&m_mutex);

  /* Create the trx sys file block in a new allocated file segment */
  auto block = m_sys.m_fsp->fseg_create(TRX_SYS_SPACE, nullptr, TRX_SYS + TRX_SYS_FSEG_HEADER, &mtr);

  buf_block_dbg_add_level();

  ut_a(block->get_page_no() == TRX_SYS_PAGE_NO);

  auto page = block->get_frame();

  mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_TRX_SYS, MLOG_2BYTES, &mtr);

  /* Reset the doublewrite buffer magic number to zero so that we
  know that the doublewrite buffer has not yet been created (this
  suppresses a Valgrind warning) */

  mlog_write_ulint(page + SYS_DOUBLEWRITE + SYS_DOUBLEWRITE_MAGIC, 0, MLOG_4BYTES, &mtr);

  auto sys_header = read_header(&mtr);

  /* The clock starts now, and the format is of this version. */
  mlog_write_uint64(sys_header + TRX_SYS_HLC_STORE, std::max({m_clock.wall(), ut::HLC_BASE_US + 1, m_sys.m_config.m_trx.m_hlc_start}), &mtr);
  mlog_write_uint64(sys_header + TRX_SYS_FORMAT_MAGIC, TRX_SYS_FORMAT_MAGIC_N, &mtr);
  mlog_write_ulint(sys_header + TRX_SYS_FORMAT_VERSION, TRX_SYS_FORMAT_VERSION_N, MLOG_4BYTES, &mtr);
  mlog_write_uint64(sys_header + TRX_SYS_EPOCH, m_sys.m_config.m_trx.m_epoch, &mtr);
  mlog_write_ulint(sys_header + TRX_SYS_NODE, m_sys.m_config.m_trx.m_node_id, MLOG_4BYTES, &mtr);

  /* Reset the rollback segment slots */
  for (ulint i{}; i < TRX_SYS_N_RSEGS; ++i) {
    frseg_set_space(sys_header, i, ULINT32_UNDEFINED, &mtr);
    frseg_set_page_no(sys_header, i, FIL_NULL, &mtr);
  }

  /* The remaining area (up to the page trailer) is uninitialized.
  Silence Valgrind warnings about it. */
  UNIV_MEM_VALID(
    sys_header + (TRX_SYS_NODE + 4),
    (UNIV_PAGE_SIZE - FIL_PAGE_DATA_END - (TRX_SYS_NODE + 4)) + page - sys_header
  );

  /* Create the first rollback segment in the SYSTEM tablespace */
  ulint slot_no;
  auto page_no = TrxRseg::header_create(m_sys, TRX_SYS_SPACE, ULINT_MAX, &slot_no, &mtr);

  ut_a(slot_no == TRX_SYS_SYSTEM_RSEG_ID);
  ut_a(page_no != FIL_NULL);

  mutex_exit(&m_mutex);

  mtr.commit(m_sys);

  return DB_SUCCESS;
}

Trx *TrxSys::create_trx(void *arg) noexcept {
  /* There is a circular reference between the session and the transaction. */
  auto session = Session::create(nullptr);
  void *mem{};
  CondVar *event{};

  {
    std::lock_guard<ut::Latch> guard(m_trx_pool_mutex);

    if (!m_trx_pool.empty()) {
      std::tie(mem, event) = m_trx_pool.back();
      m_trx_pool.pop_back();
    }
  }

  if (mem == nullptr) {
    mem = mem_alloc(sizeof(Trx));
  }

  auto trx = new (mem) Trx(session, arg, event);

  session->m_trx = trx;

  return trx;
}

void TrxSys::destroy_trx(Trx *&trx) noexcept {
  ut_a(trx->m_sess->m_trx == trx);

  trx->m_sess->m_trx = nullptr;

  Session::destroy(trx->m_sess);

  trx->m_sess = nullptr;

  ut_a(trx->m_n_ref.load() == 0);

  /* Its event stays with the memory: creating one is not cheap. */
  auto event = trx->m_wait_event;

  trx->m_wait_event = nullptr;

  call_destructor(trx);

  /* The memory goes back to the pool, not to the allocator (type stable). */
  {
    std::lock_guard<ut::Latch> guard(m_trx_pool_mutex);

    m_trx_pool.emplace_back(trx, event);
  }

  trx = nullptr;
}

Trx *TrxSys::create_user_trx(void *arg) noexcept {
  auto trx = create_trx(arg);

  ++m_n_user_trx;

  auto &shard = m_client_shards[(reinterpret_cast<uintptr_t>(trx) >> 6) % m_client_shards.size()];
  std::lock_guard<ut::Latch> guard(shard.m_latch);

  shard.m_trxs.push_front(trx);

  return trx;
}

void TrxSys::destroy_user_trx(Trx *&trx) noexcept {
  {
    auto &shard = m_client_shards[(reinterpret_cast<uintptr_t>(trx) >> 6) % m_client_shards.size()];
    std::lock_guard<ut::Latch> guard(shard.m_latch);

    shard.m_trxs.remove(trx);
  }

  const auto n = m_n_user_trx.fetch_sub(1);
  ut_a(n > 0);

  /* Off the lists (the memory goes to the pool, type stable). */
  destroy_trx(trx);
}

Trx *TrxSys::create_background_trx(void *arg) noexcept {
  auto trx = create_trx(arg);

  ++m_n_background_trx;

  return trx;
}

void TrxSys::destroy_background_trx(Trx *&trx) noexcept {
  const auto n = m_n_background_trx.fetch_sub(1);
  ut_a(n > 0);

  destroy_trx(trx);
}

void TrxSys::trx_list_insert_ordered(Trx *in_trx) noexcept {
  ut_ad(mutex_own(&m_mutex));

  Trx *prev_trx{};

  for (auto trx : m_recv_trx_list) {
    if (in_trx->m_id >= trx->m_id) {
      ut_ad(in_trx->m_id > trx->m_id);
      prev_trx = trx;
      break;
    }
  }

  if (prev_trx != nullptr) {
    /* in_trx goes before prev_trx, the first with a smaller id. */
    auto trx = UT_LIST_GET_PREV(m_trx_list, prev_trx);

    if (trx == nullptr) {
      m_recv_trx_list.push_front(in_trx);
    } else {
      UT_LIST_INSERT_AFTER(m_recv_trx_list, trx, in_trx);
    }
  } else {
    m_recv_trx_list.push_back(in_trx);
  }

  in_trx->m_in_recv_list = true;

  /* An active or prepared recovered transaction owns implicit locks, and a view
  must not see it: in the commit table from here on. One that is committed in
  memory has no entry: not found, visible. */
  if (in_trx->m_conc_state == TRX_ACTIVE || in_trx->m_conc_state == TRX_PREPARED) {
    auto node = m_nodes.alloc();

    ut_a(node != nullptr);

    node->m_ticket.store(in_trx->m_id, std::memory_order_relaxed);
    node->m_payload.m_no.store(in_trx->m_no, std::memory_order_relaxed);
    node->m_payload.m_trx.store(in_trx, std::memory_order_relaxed);

    m_nodes.insert_ordered(node);

    in_trx->m_node = node;

    ids_insert(in_trx->m_id, node);
  }

  refresh_low_water();
}

ulint TrxSys::trx_assign_rseg() noexcept {
  /* The list is fixed once the instance has started. */
  auto rseg = m_latest_rseg.load(std::memory_order_relaxed);
  const auto n_rsegs = m_rseg_list.size();

  for (;;) {
    /* Get next rseg in a round-robin fashion */

    rseg = UT_LIST_GET_NEXT(m_rseg_list, rseg);

    if (rseg == nullptr) {
      rseg = m_rseg_list.front();
    }

    /* If it is the SYSTEM rollback segment, and there exist others, skip it */

    if ((rseg->m_id != TRX_SYS_SYSTEM_RSEG_ID) || n_rsegs == 1) {
      m_latest_rseg.store(rseg, std::memory_order_relaxed);
      return rseg->m_id;
    }
  }
}

void TrxSys::init_at_db_start(IbRecovery recovery) noexcept {
  ut_ad(mutex_own(&m_mutex));

  /* Look from the rollback segments if there exist undo logs for
  transactions */

  for (auto rseg : m_rseg_list) {
    for (auto undo : rseg->m_insert_undo_list) {

      auto trx = create_trx(nullptr);

      trx->m_is_recovered = true;
      trx->m_id = undo->m_trx_id;

#ifdef WITH_XOPEN
      trx->m_xid = undo->m_xid;
#endif /* WITH_XOPEN */

      trx->m_rseg = rseg;
      trx->m_insert_undo = undo;

      if (undo->m_state != TRX_UNDO_ACTIVE) {

        /* Prepared transactions are left in the prepared state waiting for a commit
        or abort decision from the client. */

        if (undo->m_state == TRX_UNDO_PREPARED) {

          log_info(std::format("Transaction {} was in the XA prepared state.", trx->m_id));

          if (recovery == IB_RECOVERY_DEFAULT) {

            trx->m_conc_state = TRX_PREPARED;
          } else {
            log_info("Since force_recovery > 0, we will do a rollback anyway.");

            trx->m_conc_state = TRX_ACTIVE;
          }
        } else {
          trx->m_conc_state = TRX_COMMITTED_IN_MEMORY;
        }

        /* We give a dummy value for the trx no; this should have no relevance since purge
        is not interested in committed transaction numbers, unless they are in the history
        list, in which case it looks the number from the disk based undo log structure */

        trx->m_no = trx->m_id;
      } else {
        trx->m_conc_state = TRX_ACTIVE;
        /* A running transaction always has the number field inited to LSN_MAX */
        trx->m_no = LSN_MAX;
      }

      if (undo->m_dict_operation) {
        trx->set_dict_operation(TRX_DICT_OP_TABLE);
        trx->m_table_id = undo->m_table_id;
      }

      if (!undo->m_empty) {
        trx->m_undo_no = undo->m_top_undo_no + 1;
      }

      trx_list_insert_ordered(trx);
    }

    for (auto undo : rseg->m_update_undo_list) {

      /* The mutex is held, and only the recovered are there. */
      Trx *trx{};

      for (auto recv_trx : m_recv_trx_list) {
        if (recv_trx->m_id == undo->m_trx_id) {
          trx = recv_trx;
          break;
        }
      }

      if (trx == nullptr) {
        trx = create_trx(nullptr);

        trx->m_is_recovered = true;
        trx->m_id = undo->m_trx_id;

#ifdef WITH_XOPEN
        trx->m_xid = undo->m_xid;
#endif /* WITH_XOPEN */

        if (undo->m_state != TRX_UNDO_ACTIVE) {

          /* Prepared transactions are left in the prepared state waiting for a
          commit or abort decision from the client. */

          if (undo->m_state == TRX_UNDO_PREPARED) {
            log_info(std::format("Transaction {} was in the XA prepared state.", trx->m_id));

            if (recovery == IB_RECOVERY_DEFAULT) {
              trx->m_conc_state = TRX_PREPARED;
            } else {
              log_info("Since force_recovery > 0, we will do a rollback anyway.");

              trx->m_conc_state = TRX_ACTIVE;
            }
          } else {
            trx->m_conc_state = TRX_COMMITTED_IN_MEMORY;
          }

          /* We give a dummy value for the trx number */

          trx->m_no = trx->m_id;
        } else {
          trx->m_conc_state = TRX_ACTIVE;
          /* A running transaction always has the number field inited to LSN_MAX */
          trx->m_no = LSN_MAX;
        }

        trx->m_rseg = rseg;

        trx_list_insert_ordered(trx);

        if (undo->m_dict_operation) {
          trx->set_dict_operation(TRX_DICT_OP_TABLE);
          trx->m_table_id = undo->m_table_id;
        }
      }

      trx->m_update_undo = undo;

      if (!undo->m_empty && undo->m_top_undo_no >= trx->m_undo_no) {

        trx->m_undo_no = undo->m_top_undo_no + 1;
      }
    }
  }
}

int TrxSys::recover(Xid *xid_list, ulint len) noexcept {
  ut_ad(len > 0);

  /* We should set those transactions which are in the prepared state
  to the xid_list */

  int count{};

  for (const auto id : active_ids()) {
    /* Pinned: it can't reach C4 of its commit meanwhile. */
    auto trx = pin_active(id);

    if (trx == nullptr) {
      continue;
    }

    if (trx->m_conc_state == TRX_PREPARED) {
#ifdef WITH_XOPEN
      xid_list[count] = trx->m_xid;
      if (count == 0) {
        log_info("Starting recovery for XA transactions...");
      }
#endif /* WITH_XOPEN */

      log_info(std::format("Transaction {} in prepared state after recovery", trx->m_id));
      log_info(std::format("Transaction contains changes to {} rows", trx->m_undo_no));

      ++count;
    }

    unpin(trx);

    if (count == int(len)) {
      break;
    }
  }

  if (count > 0) {
    log_info(std::format("{} transactions in prepared state after recovery", count));
  }

  return count;
}

#ifdef WITH_XOPEN
Trx *TrxSys::get_trx_by_xid(Xid *xid) noexcept {
  ut_a(xid != nullptr);

  Trx *xid_trx{};

  for (const auto id : active_ids()) {
    auto trx = pin_active(id);

    if (trx == nullptr) {
      continue;
    }

    /* Compare two X/Open XA transaction id's: their
    length should be the same and binary comparison
    of gtrid_lenght+bqual_length bytes should be
    the same */

    if (trx->m_conc_state == TRX_PREPARED && xid->gtrid_length == trx->m_xid.gtrid_length && xid->bqual_length == trx->m_xid.bqual_length &&
        memcmp(xid->data, trx->m_xid.data, xid->gtrid_length + xid->bqual_length) == 0) {
      xid_trx = trx;
    }

    unpin(trx);

    if (xid_trx != nullptr) {
      break;
    }
  }

  return xid_trx;
}
#endif /* WITH_XOPEN */

void TrxSys::close_read_view(ReadView *view) {
  /* Purge's has none. */
  if (view->m_slot != nullptr) {
    m_snaps.free(view->m_slot);

    view->m_slot = nullptr;
  }
}

void TrxSys::close_read_view_for_read_committed(Trx *trx) {
  ut_a(trx->m_global_read_view != nullptr);

  close_read_view(trx->m_global_read_view);

  mem_heap_empty(trx->m_global_read_view_heap);

  trx->m_read_view = nullptr;
  trx->m_global_read_view = nullptr;
}

void TrxSys::set_cursor_view(Trx *trx, CursorView *cursor_view) {
  if (likely(cursor_view != nullptr)) {
    trx->m_read_view = cursor_view->read_view;
  } else {
    trx->m_read_view = trx->m_global_read_view;
  }
}

void TrxSys::open_snapshot(ReadView *view) noexcept {
  auto slot = m_snaps.alloc();

  if (m_view_hook != nullptr) {
    m_view_hook(m_view_hook_arg, 0);
  }

  /* The order is the proof of READ VIEWS: the slot with a lower bound, in the total
  order; then W; then S. */
  slot->m_v.exchange(m_snap_floor.load(std::memory_order_acquire), std::memory_order_seq_cst);

  if (m_view_hook != nullptr) {
    m_view_hook(m_view_hook_arg, 1);
  }

  view->m_watermark = m_low_water.load(std::memory_order_acquire);
  view->m_snap_word = m_clock.read_word();
  view->m_low_limit_no = ut::word_id_limit(view->m_snap_word);
  view->m_slot = slot;
  view->m_trx_sys = this;

  slot->m_v.store(view->m_snap_word, std::memory_order_release);
}

CursorView *TrxSys::create_cursor_view(Trx *cr_trx) {
  /* Use larger heap than in trx_create when creating a read_view because cursors are quite big. */

  auto heap = mem_heap_create(512);

  auto curview = reinterpret_cast<CursorView *>(mem_heap_alloc(heap, sizeof(CursorView)));

  curview->heap = heap;

  /* Drop cursor tables from consideration when evaluating the need of
  auto-commit */
  curview->n_client_tables_in_use = cr_trx->m_n_client_tables_in_use;
  cr_trx->m_n_client_tables_in_use = 0;

  auto view = reinterpret_cast<ReadView *>(mem_heap_alloc(heap, sizeof(ReadView)));

  view->m_creator_trx_id = cr_trx->m_id;
  view->m_type = ReadViewType::HIGH_GRANULARITY;
  view->m_undo_no = cr_trx->m_undo_no;

  open_snapshot(view);

  curview->read_view = view;

  return curview;
}

void TrxSys::close_cursor_view(Trx *trx, CursorView *curview) {
  ut_a(curview->read_view);
  ut_a(curview->heap);

  /* Add cursor's tables to the global count of active tables that
  belong to this transaction */
  trx->m_n_client_tables_in_use += curview->n_client_tables_in_use;

  close_read_view(curview->read_view);
  trx->m_read_view = trx->m_global_read_view;

  mem_heap_free(curview->heap);
}

ReadView *TrxSys::open_read_view_now(trx_id_t cr_trx_id, MemHeap *heap) {
  auto view = reinterpret_cast<ReadView *>(mem_heap_alloc(heap, sizeof(ReadView)));

  view->m_creator_trx_id = cr_trx_id;
  view->m_type = ReadViewType::NORMAL;
  view->m_undo_no = 0;

  open_snapshot(view);

  return view;
}

ReadView *TrxSys::open_purge_view(MemHeap *heap) {
  auto view = reinterpret_cast<ReadView *>(mem_heap_alloc(heap, sizeof(ReadView)));

  /* A slot for the walk only: PURGE in READ VIEWS. It doesn't hold the table after
  the open. */
  auto slot = m_snaps.alloc();

  slot->m_v.exchange(m_snap_floor.load(std::memory_order_acquire), std::memory_order_seq_cst);

  const auto c0 = m_clock.read_word();
  const auto h = m_snaps.min(c0, slot);

  slot->m_v.store(h, std::memory_order_release);

  const auto limit = ut::word_id_limit(h);

  auto w = limit;
  auto l = limit;

  {
    TrxList::Scan scan(m_nodes);

    for (auto node = scan.head(); node != nullptr; node = TrxList::Scan::next(node)) {
      const auto id = scan.ticket(*node);
      const auto c = m_nodes.final_seq(*node);

      if (id < limit && (c == 0 || c > h)) {
        w = std::min(w, id);
        l = std::min(l, node->m_payload.m_no.load(std::memory_order_seq_cst));
      }
    }
  }

  view->m_creator_trx_id = 0;
  view->m_type = ReadViewType::NORMAL;
  view->m_undo_no = 0;
  view->m_snap_word = h;
  view->m_watermark = w;
  view->m_low_limit_no = l;
  view->m_slot = nullptr;
  view->m_trx_sys = this;

  m_snaps.free(slot);

  return view;
}
