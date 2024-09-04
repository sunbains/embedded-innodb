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

/** The transaction system */
Trx_sys *srv_trx_sys{};

Trx_sys::Trx_sys(FSP *fsp) noexcept
  : m_fsp(fsp) {
  UT_LIST_INIT(m_trx_list);
  UT_LIST_INIT(m_rseg_list);
  UT_LIST_INIT(m_view_list);
  UT_LIST_INIT(m_client_trx_list);
}

Trx_sys::~Trx_sys() noexcept {
  /* Check that all read views are closed except read view owned
  by a purge. */

  if (UT_LIST_GET_LEN(m_view_list) > 1) {
    log_err(std::format(
      "All read views were not closed before shutdown: {} read views open",
      UT_LIST_GET_LEN(m_view_list) - 1
    ));
  }

  sess_close(trx_dummy_sess);
  trx_dummy_sess = nullptr;

  call_destructor(m_purge);
  ut_delete(m_purge);

  /* This is required only because it's a pre-condition for many
  of the functions that we need to call. */
  mutex_enter(&kernel_mutex);

  /* There can't be any active transactions. */
  auto rseg = UT_LIST_GET_FIRST(m_rseg_list);

  while (rseg != nullptr) {
    auto prev_rseg = rseg;

    rseg = UT_LIST_GET_NEXT(rseg_list, prev_rseg);

    UT_LIST_REMOVE(m_rseg_list, prev_rseg);

    trx_rseg_mem_free(prev_rseg);
  }

  auto view = UT_LIST_GET_FIRST(m_view_list);

  while (view != nullptr) {
    auto prev_view = view;

    view = UT_LIST_GET_NEXT(view_list, prev_view);

    /* Views are allocated from the m_global_read_view_heap.
    So, we simply remove the element here. */
    UT_LIST_REMOVE(m_view_list, prev_view);
}

  ut_a(UT_LIST_GET_LEN(m_trx_list) == 0);
  ut_a(UT_LIST_GET_LEN(m_rseg_list) == 0);
  ut_a(UT_LIST_GET_LEN(m_view_list) == 0);
  ut_a(UT_LIST_GET_LEN(m_client_trx_list) == 0);

  mutex_exit(&kernel_mutex);
}

dberr_t Trx_sys::start(ib_recovery_t recovery) noexcept {
  const char *unit = "";

  mtr_t mtr;

  mtr.start();

  mutex_enter(&kernel_mutex);

  auto sys_header = read_header(&mtr);

  trx_rseg_list_and_array_init(recovery, sys_header, &mtr);

  m_latest_rseg = UT_LIST_GET_FIRST(m_rseg_list);

  /* VERY important: after the database is started, max_trx_id value is
   * divisible by TRX_SYS_TRX_ID_WRITE_MARGIN, and the 'if' in
   * trx_sys_get_new_trx_id will evaluate to true when the function
   * is first time called, and the value for trx id will be written
   * to the disk-based header! Thus trx id values will not overlap when
   * the database is repeatedly started! */

  m_max_trx_id = ut_uint64_align_up(
    mtr.read_uint64(sys_header + TRX_SYS_TRX_ID_STORE),
    TRX_SYS_TRX_ID_WRITE_MARGIN) + 2 * TRX_SYS_TRX_ID_WRITE_MARGIN;


  trx_dummy_sess = sess_open();

  trx_lists_init_at_db_start(recovery);

  int64_t rows_to_undo{};

  if (UT_LIST_GET_LEN(m_trx_list) > 0) {
    auto trx = UT_LIST_GET_FIRST(m_trx_list);

    for (;;) {

      if (trx->m_conc_state != TRX_PREPARED) {
        rows_to_undo += trx->undo_no;
      }

      trx = UT_LIST_GET_NEXT(trx_list, trx);

      if (trx == nullptr) {
        break;
      }
    }

    if (rows_to_undo > 1000000000) {
      unit = "M";
      rows_to_undo = rows_to_undo / 1000000;
    }

    log_info(std::format(
      "{} transaction(s) which must be rolled back or cleaned up"
      " in total {}s row operations to undo",
      UT_LIST_GET_LEN(m_trx_list),
      rows_to_undo,
      unit
    ));

    log_info(std::format("Trx id counter is {}", TRX_ID_PREP_PRINTF(m_max_trx_id)));
  }


  {
    ut_a(m_purge == nullptr);
    auto ptr = ut_new(sizeof(Purge_sys));
    m_purge = new (ptr) Purge_sys();
  }

  mutex_exit(&kernel_mutex);

  mtr.commit();

  return DB_SUCCESS;
}

bool Trx_sys::in_trx_list(trx_t *in_trx) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  for (auto trx : m_trx_list) {
    if (trx == in_trx) {

      return true;
    }
  }

  return false;
}

void Trx_sys::flush_max_trx_id() noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  mtr_t mtr;

  mtr.start();

  auto sys_header = read_header(&mtr);

  mlog_write_uint64(sys_header + TRX_SYS_TRX_ID_STORE, m_max_trx_id, &mtr);

  mtr.commit();
}

ulint Trx_sys::frseg_find_free(mtr_t *mtr) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  auto sys_header = read_header(mtr);

  for (ulint i{}; i < TRX_SYS_N_RSEGS; ++i) {

    const auto page_no = frseg_get_page_no(sys_header, i, mtr);

    if (page_no == FIL_NULL) {

      return i;
    }
  }

  return ULINT_UNDEFINED;
}

dberr_t Trx_sys::create_system_tablespace() noexcept {
  mtr_t mtr;

  mtr.start();

  /* Note that below we first reserve the file space x-latch, and
  then enter the kernel: we must do it in this order to conform
  to the latching order rules. */

  mtr_x_lock(m_fsp->m_fil->space_get_latch(TRX_SYS_SPACE), &mtr);

  mutex_enter(&kernel_mutex);

  /* Create the trx sys file block in a new allocated file segment */
  auto block = m_fsp->fseg_create(TRX_SYS_SPACE, 0, TRX_SYS + TRX_SYS_FSEG_HEADER, &mtr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TRX_SYS_HEADER));

  ut_a(block->get_page_no() == TRX_SYS_PAGE_NO);

  auto page = block->get_frame();

  mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_TRX_SYS, MLOG_2BYTES, &mtr);

  /* Reset the doublewrite buffer magic number to zero so that we
  know that the doublewrite buffer has not yet been created (this
  suppresses a Valgrind warning) */

  mlog_write_ulint(page + SYS_DOUBLEWRITE + SYS_DOUBLEWRITE_MAGIC, 0, MLOG_4BYTES, &mtr);

  auto sys_header = read_header(&mtr);

  /* Start counting transaction ids from number 1 up */
  mlog_write_uint64(sys_header + TRX_SYS_TRX_ID_STORE, 1, &mtr);

  /* Reset the rollback segment slots */
  for (ulint i{}; i < TRX_SYS_N_RSEGS; ++i) {
    frseg_set_space(sys_header, i, ULINT_UNDEFINED, &mtr);
    frseg_set_page_no(sys_header, i, FIL_NULL, &mtr);
  }

  /* The remaining area (up to the page trailer) is uninitialized.
  Silence Valgrind warnings about it. */
  UNIV_MEM_VALID(
    sys_header + (TRX_SYS_RSEGS + TRX_SYS_N_RSEGS * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE),
    (UNIV_PAGE_SIZE - FIL_PAGE_DATA_END - (TRX_SYS_RSEGS + TRX_SYS_N_RSEGS * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE)) + page -
      sys_header
  );

  /* Create the first rollback segment in the SYSTEM tablespace */
  ulint slot_no;
  auto page_no = trx_rseg_header_create(TRX_SYS_SPACE, ULINT_MAX, &slot_no, &mtr);

  ut_a(slot_no == TRX_SYS_SYSTEM_RSEG_ID);
  ut_a(page_no != FIL_NULL);

  mutex_exit(&kernel_mutex);

  mtr.commit();

  return DB_SUCCESS;
}

Trx_sys *Trx_sys::create(FSP *fsp) noexcept {
  auto ptr = ut_new(sizeof(Trx_sys));

  return ptr != nullptr ? new (ptr) Trx_sys(fsp) : nullptr;
}

void Trx_sys::destroy(Trx_sys *&trx_sys) noexcept {
  call_destructor(trx_sys);
  ut_delete(trx_sys);
  trx_sys = nullptr;
}

