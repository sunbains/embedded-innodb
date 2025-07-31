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

Trx_sys::Trx_sys(FSP *fsp) noexcept : m_fsp(fsp) {
  mutex_create(&m_mutex, IF_DEBUG("trx_sys_mutex", ) IF_SYNC_DEBUG(TRX_SYS_MUTEX, ) Current_location());
}

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
  mutex_enter(&m_mutex);

  /* There can't be any active transactions. */
  auto rseg = m_rseg_list.front();

  while (rseg != nullptr) {
    auto prev_rseg = rseg;

    rseg = UT_LIST_GET_NEXT(m_rseg_list, prev_rseg);

    m_rseg_list.remove(prev_rseg);

    prev_rseg->memory_free();
  }

  auto view = m_view_list.front();

  while (view != nullptr) {
    auto prev_view = view;

    view = UT_LIST_GET_NEXT(m_view_list, prev_view);

    /* Views are allocated from the m_global_read_view_heap.
    So, we simply remove the element here. */
    m_view_list.remove(prev_view);
  }

  ut_a(m_trx_list.empty());
  ut_a(m_rseg_list.empty());
  ut_a(m_view_list.empty());
  ut_a(m_client_trx_list.empty());

  mutex_exit(&m_mutex);

  mutex_free(&m_mutex);
}

dberr_t Trx_sys::start(ib_recovery_t recovery) noexcept {
  const char *unit = "";

  auto purge_trx = create_background_trx(nullptr);

  mtr_t mtr;

  mtr.start();

  mutex_enter(&m_mutex);

  auto sys_header = read_header(&mtr);

  Trx_rseg::list_and_array_init(recovery, sys_header, &mtr);

  m_latest_rseg = m_rseg_list.front();

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

  mutex_exit(&m_mutex);

  mtr.commit();

  return DB_SUCCESS;
}

bool Trx_sys::in_trx_list(Trx *in_trx) noexcept {
  ut_ad(mutex_own(&m_mutex));

  for (auto trx : m_trx_list) {
    if (trx == in_trx) {

      return true;
    }
  }

  return false;
}

void Trx_sys::flush_max_trx_id() noexcept {
  ut_ad(mutex_own(&m_mutex));

  mtr_t mtr;

  mtr.start();

  auto sys_header = read_header(&mtr);

  mlog_write_uint64(sys_header + TRX_SYS_TRX_ID_STORE, m_max_trx_id, &mtr);

  mtr.commit();
}

ulint Trx_sys::frseg_find_free(mtr_t *mtr) noexcept {
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

dberr_t Trx_sys::create_system_tablespace() noexcept {
  mtr_t mtr;

  mtr.start();

  /* Note that below we first reserve the file space x-latch, and
  then enter the kernel: we must do it in this order to conform
  to the latching order rules. */

  mtr_x_lock(m_fsp->m_fil->space_get_latch(TRX_SYS_SPACE), &mtr);

  mutex_enter(&m_mutex);

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
  auto page_no = Trx_rseg::header_create(TRX_SYS_SPACE, ULINT_MAX, &slot_no, &mtr);

  ut_a(slot_no == TRX_SYS_SYSTEM_RSEG_ID);
  ut_a(page_no != FIL_NULL);

  mutex_exit(&m_mutex);

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

  mutex_enter(&m_mutex);

  ++m_n_user_trx;

  m_client_trx_list.push_front(trx);

  mutex_exit(&m_mutex);

  return trx;
}

void Trx_sys::destroy_user_trx(Trx *&trx) noexcept {
  mutex_enter(&m_mutex);

  m_client_trx_list.remove(trx);

  destroy_trx(trx);

  ut_a(m_n_user_trx > 0);
  --m_n_user_trx;

  mutex_exit(&m_mutex);
}

Trx *Trx_sys::create_background_trx(void *arg) noexcept {
  auto trx = create_trx(arg);

  mutex_enter(&m_mutex);

  ++m_n_background_trx;

  mutex_exit(&m_mutex);

  return trx;
}

void Trx_sys::destroy_background_trx(Trx *&trx) noexcept {
  mutex_enter(&m_mutex);

  destroy_trx(trx);

  ut_a(m_n_background_trx > 0);
  --m_n_background_trx;

  mutex_exit(&m_mutex);
}

void Trx_sys::trx_list_insert_ordered(Trx *in_trx) noexcept {
  ut_ad(mutex_own(&m_mutex));

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
  ut_ad(mutex_own(&m_mutex));

  auto rseg = m_latest_rseg;
  const auto n_rsegs = m_rseg_list.size();

  for (;;) {
    /* Get next rseg in a round-robin fashion */

    rseg = UT_LIST_GET_NEXT(m_rseg_list, rseg);

    if (rseg == nullptr) {
      rseg = m_rseg_list.front();
    }

    /* If it is the SYSTEM rollback segment, and there exist others, skip it */

    if ((rseg->m_id != TRX_SYS_SYSTEM_RSEG_ID) || n_rsegs == 1) {
      m_latest_rseg = rseg;
      return rseg->m_id;
    }
  }
}

void Trx_sys::init_at_db_start(ib_recovery_t recovery) noexcept {
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
    }
  }
}

int Trx_sys::recover(XID *xid_list, ulint len) noexcept {
  ut_ad(len > 0);

  /* We should set those transactions which are in the prepared state
  to the xid_list */

  int count{};

  mutex_enter(&m_mutex);

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

  mutex_exit(&m_mutex);

  if (count > 0) {
    log_info(std::format("{} transactions in prepared state after recovery", count));
  }

  return count;
}

#ifdef WITH_XOPEN
Trx *Trx_sys::get_trx_by_xid(XID *xid) noexcept {
  ut_a(xid != nullptr);

  mutex_enter(&m_mutex);

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

  mutex_exit(&m_mutex);

  return xid_trx == nullptr ? nullptr : (xid_trx->m_conc_state != TRX_PREPARED ? nullptr : xid_trx);
}
#endif /* WITH_XOPEN */

void Trx_sys::close_read_view(Read_view *view) {
  ut_ad(mutex_own(&m_mutex));

  m_view_list.remove(view);
}

void Trx_sys::close_read_view_for_read_committed(Trx *trx) {
  ut_a(trx->m_global_read_view != nullptr);

  mutex_enter(&m_mutex);

  close_read_view(trx->m_global_read_view);

  mem_heap_empty(trx->m_global_read_view_heap);

  trx->m_read_view = nullptr;
  trx->m_global_read_view = nullptr;

  mutex_exit(&m_mutex);
}

void Trx_sys::set_cursor_view(Trx *trx, Cursor_view *cursor_view) {
  mutex_enter(&m_mutex);

  if (likely(cursor_view != nullptr)) {
    trx->m_read_view = cursor_view->read_view;
  } else {
    trx->m_read_view = trx->m_global_read_view;
  }

  mutex_exit(&m_mutex);
}

Cursor_view *Trx_sys::create_cursor_view(Trx *cr_trx) {
  /* Use larger heap than in trx_create when creating a read_view because cursors are quite big. */

  auto heap = mem_heap_create(512);

  auto curview = reinterpret_cast<Cursor_view *>(mem_heap_alloc(heap, sizeof(Cursor_view)));

  curview->heap = heap;

  /* Drop cursor tables from consideration when evaluating the need of
  auto-commit */
  curview->n_client_tables_in_use = cr_trx->m_n_client_tables_in_use;
  cr_trx->m_n_client_tables_in_use = 0;

  mutex_enter(&m_mutex);

  curview->read_view = create_read_view_low(m_trx_list.size(), curview->heap);

  auto view = curview->read_view;
  view->m_creator_trx_id = cr_trx->m_id;
  view->m_type = Read_view_type::HIGH_GRANULARITY;
  view->m_undo_no = cr_trx->m_undo_no;

  /* No future transactions should be visible in the view */

  view->m_low_limit_no = m_max_trx_id;
  view->m_low_limit_id = view->m_low_limit_no;

  ulint n = 0;
  /* No active transaction should be visible */

  for (auto trx : m_trx_list) {
    if (trx->m_conc_state == TRX_ACTIVE || trx->m_conc_state == TRX_PREPARED) {

      view->set_nth_trx_id(n, trx->m_id);

      n++;

      /* NOTE that a transaction whose trx number is <
      srv_trx_sys->m_max_trx_id can still be active, if it is
      in the middle of its commit! Note that when a
      transaction starts, we initialize trx->no to
      LSN_MAX. */

      if (view->m_low_limit_no > trx->m_no) {

        view->m_low_limit_no = trx->m_no;
      }
    }
  }

  view->m_n_trx_ids = n;

  if (n > 0) {
    /* The last active transaction has the smallest id: */
    view->m_up_limit_id = view->get_nth_trx_id(n - 1);
  } else {
    view->m_up_limit_id = view->m_low_limit_id;
  }

  m_view_list.push_front(view);

  mutex_exit(&m_mutex);

  return curview;
}

void Trx_sys::close_cursor_view(Trx *trx, Cursor_view *curview) {
  ut_a(curview->read_view);
  ut_a(curview->heap);

  /* Add cursor's tables to the global count of active tables that
  belong to this transaction */
  trx->m_n_client_tables_in_use += curview->n_client_tables_in_use;

  mutex_enter(&m_mutex);

  close_read_view(curview->read_view);
  trx->m_read_view = trx->m_global_read_view;

  mutex_exit(&m_mutex);

  mem_heap_free(curview->heap);
}

Read_view *Trx_sys::create_read_view_low(ulint n, mem_heap_t *heap) {
  auto view = reinterpret_cast<Read_view *>(mem_heap_alloc(heap, sizeof(Read_view)));

  view->m_n_trx_ids = n;
  view->m_trx_ids = reinterpret_cast<trx_id_t *>(mem_heap_alloc(heap, n * sizeof *view->m_trx_ids));

  return view;
}

Read_view *Trx_sys::oldest_copy_or_open_new(trx_id_t cr_trx_id, mem_heap_t *heap) {
  ut_ad(mutex_own(&m_mutex));

  auto old_view = m_view_list.back();

  if (old_view == nullptr) {

    return open_read_view_now(cr_trx_id, heap);
  }

  auto n = old_view->m_n_trx_ids;

  if (old_view->m_creator_trx_id > 0) {
    ++n;
  } else {
    n = 0;
  }

  auto view_copy = create_read_view_low(n, heap);

  /* Insert the id of the creator in the right place of the descending
  array of ids, if needs_insert is true: */

  ulint insert_done{0};
  bool needs_insert{true};

  for (ulint i{}; i < n; ++i) {
    if (needs_insert && (i >= old_view->m_n_trx_ids || old_view->m_creator_trx_id > old_view->get_nth_trx_id(i))) {

      view_copy->set_nth_trx_id(i, old_view->m_creator_trx_id);
      needs_insert = false;
      insert_done = 1;
    } else {
      view_copy->set_nth_trx_id(i, old_view->get_nth_trx_id(i - insert_done));
    }
  }

  view_copy->m_creator_trx_id = cr_trx_id;

  view_copy->m_low_limit_no = old_view->m_low_limit_no;
  view_copy->m_low_limit_id = old_view->m_low_limit_id;

  if (n > 0) {
    /* The last active transaction has the smallest id: */
    view_copy->m_up_limit_id = view_copy->get_nth_trx_id(n - 1);
  } else {
    view_copy->m_up_limit_id = old_view->m_up_limit_id;
  }

  m_view_list.push_back(view_copy);

  return view_copy;
}

Read_view *Trx_sys::open_read_view_now(trx_id_t cr_trx_id, mem_heap_t *heap) {
  ut_ad(mutex_own(&m_mutex));

  auto view = create_read_view_low(m_trx_list.size(), heap);

  view->m_creator_trx_id = cr_trx_id;
  view->m_type = Read_view_type::NORMAL;
  view->m_undo_no = 0;

  /* No future transactions should be visible in the view */

  view->m_low_limit_no = m_max_trx_id;
  view->m_low_limit_id = view->m_low_limit_no;

  ulint n{};

  /* No active transaction should be visible, except cr_trx */
  for (auto trx : m_trx_list) {
    ut_ad(trx->m_magic_n == TRX_MAGIC_N);

    if (trx->m_id != cr_trx_id && (trx->m_conc_state == TRX_ACTIVE || trx->m_conc_state == TRX_PREPARED)) {

      view->set_nth_trx_id(n, trx->m_id);

      ++n;

      /* NOTE that a transaction whose trx number is < srv_trx_sys->m_max_trx_id can still be active, if it is
      in the middle of its commit! Note that when a transaction starts, we initialize trx->no to LSN_MAX. */

      if (view->m_low_limit_no > trx->m_no) {

        view->m_low_limit_no = trx->m_no;
      }
    }
  }

  view->m_n_trx_ids = n;

  if (n > 0) {
    /* The last active transaction has the smallest id: */
    view->m_up_limit_id = view->get_nth_trx_id(n - 1);
  } else {
    view->m_up_limit_id = view->m_low_limit_id;
  }

  m_view_list.push_front(view);

  return view;
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
