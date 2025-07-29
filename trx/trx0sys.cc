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

Trx_sys::Trx_sys(FSP *fsp) noexcept : m_fsp(fsp) {}

Trx_sys::~Trx_sys() noexcept {
  /* Check that all read views are closed except read view owned
  by a purge. */

  if (m_view_list.size() > 1) {
    log_err(std::format("All read views were not closed before shutdown: {} read views open", m_view_list.size() - 1));
  }

  auto purge_trx = m_purge->m_trx;

  call_destructor(m_purge);
  ut_delete(m_purge);

  destroy_background_trx(purge_trx);

  /* This is required only because it's a pre-condition for many
  of the functions that we need to call. */
  mutex_enter(&kernel_mutex);

  /* There can't be any active transactions. */
  auto rseg = m_rseg_list.front();

  while (rseg != nullptr) {
    auto prev_rseg = rseg;

    rseg = UT_LIST_GET_NEXT(rseg_list, prev_rseg);

    m_rseg_list.remove(prev_rseg);

    trx_rseg_mem_free(prev_rseg);
  }

  auto view = m_view_list.front();

  while (view != nullptr) {
    auto prev_view = view;

    view = UT_LIST_GET_NEXT(view_list, prev_view);

    /* Views are allocated from the m_global_read_view_heap.
    So, we simply remove the element here. */
    m_view_list.remove(prev_view);
  }

  ut_a(m_trx_list.empty());
  ut_a(m_rseg_list.empty());
  ut_a(m_view_list.empty());
  ut_a(m_client_trx_list.empty());

  mutex_exit(&kernel_mutex);
}

dberr_t Trx_sys::start(ib_recovery_t recovery) noexcept {
  const char *unit = "";

  auto purge_trx = create_background_trx(nullptr);

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

  m_max_trx_id = ut_uint64_align_up(mtr.read_uint64(sys_header + TRX_SYS_TRX_ID_STORE), TRX_SYS_TRX_ID_WRITE_MARGIN) +
                 2 * TRX_SYS_TRX_ID_WRITE_MARGIN;

  init_at_db_start(recovery);

  int64_t rows_to_undo{};

  if (!m_trx_list.empty()) {
    for (auto trx : m_trx_list) {
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
      m_trx_list.size(),
      rows_to_undo,
      unit
    ));

    log_info(std::format("Trx id counter is {}", m_max_trx_id));
  }

  {
    ut_a(m_purge == nullptr);

    auto ptr = ut_new(sizeof(Purge_sys));
    m_purge = new (ptr) Purge_sys(purge_trx);
  }

  mutex_exit(&kernel_mutex);

  mtr.commit();

  return DB_SUCCESS;
}

bool Trx_sys::in_trx_list(Trx *in_trx) noexcept {
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
    frseg_set_space(sys_header, i, ULINT32_UNDEFINED, &mtr);
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

Trx *Trx_sys::create_trx(void *arg) noexcept {
  /* There is a circular reference between the session and the transaction. */
  auto session = Session::create(nullptr);
  auto trx = Trx::create(this, session, arg);

  session->m_trx = trx;

  return trx;
}

void Trx_sys::destroy_trx(Trx *&trx) noexcept {
  ut_a(trx->m_sess->m_trx == trx);

  trx->m_sess->m_trx = nullptr;

  Session::destroy(trx->m_sess);

  trx->m_sess = nullptr;

  Trx::destroy(trx);

  trx = nullptr;
}

Trx *Trx_sys::create_user_trx(void *arg) noexcept {
  auto trx = create_trx(arg);

  mutex_enter(&kernel_mutex);

  ++m_n_user_trx;

  m_client_trx_list.push_front(trx);

  mutex_exit(&kernel_mutex);

  return trx;
}

void Trx_sys::destroy_user_trx(Trx *&trx) noexcept {
  mutex_enter(&kernel_mutex);

  m_client_trx_list.remove(trx);

  destroy_trx(trx);

  ut_a(m_n_user_trx > 0);
  --m_n_user_trx;

  mutex_exit(&kernel_mutex);
}

Trx *Trx_sys::create_background_trx(void *arg) noexcept {
  auto trx = create_trx(arg);

  mutex_enter(&kernel_mutex);

  ++m_n_background_trx;

  mutex_exit(&kernel_mutex);

  return trx;
}

void Trx_sys::destroy_background_trx(Trx *&trx) noexcept {
  mutex_enter(&kernel_mutex);

  destroy_trx(trx);

  ut_a(m_n_background_trx > 0);
  --m_n_background_trx;

  mutex_exit(&kernel_mutex);
}

void Trx_sys::trx_list_insert_ordered(Trx *in_trx) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  Trx *prev_trx{};

  for (auto trx : m_trx_list) {
    if (in_trx->m_id >= trx->m_id) {
      ut_ad(in_trx->m_id > trx->m_id);
      prev_trx = trx;
      break;
    }
  }

  if (prev_trx != nullptr) {
    auto trx = UT_LIST_GET_PREV(m_trx_list, prev_trx);

    if (trx == nullptr) {
      m_trx_list.push_front(in_trx);
    } else {
      UT_LIST_INSERT_AFTER(m_trx_list, prev_trx, in_trx);
    }
  } else {
    m_trx_list.push_back(in_trx);
  }
}

ulint Trx_sys::trx_assign_rseg() noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  auto rseg = m_latest_rseg;
  const auto n_rsegs = m_rseg_list.size();

  for (;;) {
    /* Get next rseg in a round-robin fashion */

    rseg = UT_LIST_GET_NEXT(rseg_list, rseg);

    if (rseg == nullptr) {
      rseg = m_rseg_list.front();
    }

    /* If it is the SYSTEM rollback segment, and there exist others, skip it */

    if ((rseg->id != TRX_SYS_SYSTEM_RSEG_ID) || n_rsegs == 1) {
      m_latest_rseg = rseg;
      return rseg->id;
    }
  }
}

void Trx_sys::init_at_db_start(ib_recovery_t recovery) noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  /* Look from the rollback segments if there exist undo logs for
  transactions */

  auto rseg = m_rseg_list.front();

  while (rseg != nullptr) {
    auto undo = rseg->insert_undo_list.front();

    while (undo != nullptr) {

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

      undo = UT_LIST_GET_NEXT(m_undo_list, undo);
    }

    undo = rseg->update_undo_list.front();

    while (undo != nullptr) {
      auto trx = get_on_id(undo->m_trx_id);

      if (trx == nullptr) {
        trx = Trx::create(this, nullptr, nullptr);

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

      undo = UT_LIST_GET_NEXT(m_undo_list, undo);
    }

    rseg = UT_LIST_GET_NEXT(rseg_list, rseg);
  }
}

int Trx_sys::recover(XID *xid_list, ulint len) noexcept {
  ut_ad(len > 0);

  /* We should set those transactions which are in the prepared state
  to the xid_list */

  int count{};

  mutex_enter(&kernel_mutex);

  for (const auto trx : m_trx_list) {
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

      if (count == int(len)) {
        break;
      }
    }
  }

  mutex_exit(&kernel_mutex);

  if (count > 0) {
    log_info(std::format("{} transactions in prepared state after recovery", count));
  }

  return count;
}

#ifdef WITH_XOPEN
Trx *Trx_sys::get_trx_by_xid(XID *xid) noexcept {
  ut_a(xid != nullptr);

  mutex_enter(&kernel_mutex);

  Trx *xid_trx{};

  for (auto trx : m_trx_list) {
    /* Compare two X/Open XA transaction id's: their
    length should be the same and binary comparison
    of gtrid_lenght+bqual_length bytes should be
    the same */

    if (xid->gtrid_length == trx->m_xid.gtrid_length && xid->bqual_length == trx->m_xid.bqual_length &&
        memcmp(xid->data, trx->m_xid.data, xid->gtrid_length + xid->bqual_length) == 0) {
      xid_trx = trx;
      break;
    }
  }

  mutex_exit(&kernel_mutex);

  return xid_trx == nullptr ? nullptr : (xid_trx->m_conc_state != TRX_PREPARED ? nullptr : xid_trx);
}
#endif /* WITH_XOPEN */

Trx_sys *Trx_sys::create(FSP *fsp) noexcept {
  auto ptr = ut_new(sizeof(Trx_sys));

  return ptr != nullptr ? new (ptr) Trx_sys(fsp) : nullptr;
}

void Trx_sys::destroy(Trx_sys *&trx_sys) noexcept {
  call_destructor(trx_sys);

  ut_delete(trx_sys);

  trx_sys = nullptr;
}
