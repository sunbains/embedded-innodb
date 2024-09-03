/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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

/** @file trx/trx0purge.c
Purge old versions

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#include "trx0purge.h"

#include "fsp0fsp.h"
#include "fut0fut.h"
#include "mach0data.h"
#include "mtr0log.h"
#include "os0thread.h"
#include "que0que.h"
#include "read0read.h"
#include "row0purge.h"
#include "row0upd.h"
#include "trx0rec.h"
#include "trx0roll.h"
#include "trx0rseg.h"
#include "trx0trx.h"

/** A dummy undo record used as a return value when we have a whole undo log
which needs no purge */
trx_undo_rec_t trx_purge_dummy_rec;

/**
 * Gets the biggest pair of a trx number and an undo number in a purge array.
 * 
 * @param[in] arr               Purge array
 * @param[out] trx_no           Transaction number: 0 if array is empty.
 * @param[out] undo_no          Undo number.
 */
static void trx_purge_arr_get_biggest(trx_undo_arr_t *arr, trx_id_t *trx_no, undo_no_t *undo_no) noexcept {
  ulint n{};
  trx_id_t pair_trx_no{};
  undo_no_t pair_undo_no{};
  auto n_used = arr->n_used;

  for (ulint i = 0;; ++i) {
    auto cell = trx_undo_arr_get_nth_info(arr, i);

    if (cell->in_use) {
      ++n;

      int trx_cmp = cell->trx_no - pair_trx_no;

      if (trx_cmp > 0 || (trx_cmp == 0 && cell->undo_no >= pair_undo_no)) {

        pair_trx_no = cell->trx_no;
        pair_undo_no = cell->undo_no;
      }
    }

    if (n == n_used) {
      *trx_no = pair_trx_no;
      *undo_no = pair_undo_no;

      return;
    }
  }
}

trx_undo_inf_t *Purge_sys::arr_store_info(trx_id_t trx_no, undo_no_t undo_no) noexcept {
  for (ulint i{};; ++i) {
    auto cell = trx_undo_arr_get_nth_info(m_arr, i);

    if (!(cell->in_use)) {
      /* Not in use, we may store here */
      cell->undo_no = undo_no;
      cell->trx_no = trx_no;
      cell->in_use = true;

      ++m_arr->n_used;

      return cell;
    }
  }
}

void Purge_sys::arr_remove_info(trx_undo_inf_t *cell) noexcept {
  auto arr = m_arr;

  cell->in_use = false;

  ut_ad(arr->n_used > 0);

  --arr->n_used;
}

que_t *Purge_sys::graph_build() noexcept {
  auto heap = mem_heap_create(512);
  auto fork = que_fork_create(nullptr, nullptr, QUE_FORK_PURGE, heap);

  fork->trx = m_trx;

  auto thr = que_thr_create(fork, heap);

  thr->child = row_purge_node_create(thr, heap);

  /*
  auto thr2 = que_thr_create(fork, fork, heap);

  thr2->child = row_purge_node_create(fork, thr2, heap);
  */

  return fork;
}

void Purge_sys::free_segment(trx_rseg_t *rseg, fil_addr_t hdr_addr, ulint n_removed_logs) noexcept {
  ut_ad(mutex_own(&m_mutex));

  mtr_t mtr;
  bool marked{};
  trx_ulogf_t *log_hdr{};
  trx_usegf_t *seg_hdr{};
  trx_rsegf_t *rseg_hdr{};

  for (;;) {

    mtr.start();

    mutex_enter(&rseg->mutex);

    rseg_hdr = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);

    auto undo_page = srv_undo->page_get(rseg->space, hdr_addr.m_page_no, &mtr);

    seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
    log_hdr = undo_page + hdr_addr.m_boffset;

    /* Mark the last undo log totally purged, so that if the system
    crashes, the tail of the undo log will not get accessed again. The
    list of pages in the undo log tail gets inconsistent during the
    freeing of the segment, and therefore purge should not try to access
    them again. */

    if (!marked) {
      mlog_write_ulint(log_hdr + TRX_UNDO_DEL_MARKS, false, MLOG_2BYTES, &mtr);
      marked = true;
    }

    if (srv_fsp->fseg_free_step_not_header(seg_hdr + TRX_UNDO_FSEG_HEADER, &mtr)) {
      break;
    }

    mutex_exit(&rseg->mutex);

    mtr.commit();
  }

  /* The page list may now be inconsistent, but the length field
  stored in the list base node tells us how big it was before we
  started the freeing. */

  auto seg_size = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST, &mtr);

  /* We may free the undo log segment header page; it must be freed
  within the same mtr as the undo log header is removed from the
  history list: otherwise, in case of a database crash, the segment
  could become inaccessible garbage in the file space. */

  flst_cut_end(rseg_hdr + TRX_RSEG_HISTORY, log_hdr + TRX_UNDO_HISTORY_NODE, n_removed_logs, &mtr);

  mutex_enter(&kernel_mutex);

  ut_ad(trx_sys->rseg_history_len >= n_removed_logs);
  trx_sys->rseg_history_len -= n_removed_logs;

  mutex_exit(&kernel_mutex);

  do {
    /* Here we assume that a file segment with just the header
    page can be freed in a few steps, so that the buffer pool
    is not flooded with bufferfixed pages: see the note in
    fsp0fsp.c. */
  } while (srv_fsp->fseg_free_step(seg_hdr + TRX_UNDO_FSEG_HEADER, &mtr));

  auto hist_size = mtr.read_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES);
  ut_ad(hist_size >= seg_size);

  mlog_write_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, hist_size - seg_size, MLOG_4BYTES, &mtr);

  ut_ad(rseg->curr_size >= seg_size);

  rseg->curr_size -= seg_size;

  mutex_exit(&rseg->mutex);

  mtr.commit();
}

void Purge_sys::truncate_rseg_history(trx_rseg_t *rseg, trx_id_t limit_trx_no, undo_no_t limit_undo_no) noexcept {
  ulint n_removed_logs{};

  ut_ad(mutex_own(&m_mutex));

  mtr_t mtr;

  mtr.start();

  mutex_enter(&rseg->mutex);

  auto rseg_hdr = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);
  auto hdr_addr = trx_purge_get_log_from_hist(flst_get_last(rseg_hdr + TRX_RSEG_HISTORY, &mtr));

  for (;;) {

    if (hdr_addr.m_page_no == FIL_NULL) {

      mutex_exit(&rseg->mutex);

      mtr.commit();

      return;
    }

    auto undo_page = srv_undo->page_get(rseg->space, hdr_addr.m_page_no, &mtr);
    auto log_hdr = undo_page + hdr_addr.m_boffset;
    auto cmp = mach_read_from_8(log_hdr + TRX_UNDO_TRX_NO) - limit_trx_no;

    if (cmp == 0) {
      srv_undo->truncate_start(rseg, rseg->space, hdr_addr.m_page_no, hdr_addr.m_boffset, limit_undo_no);
    }

    if (cmp >= 0) {
      mutex_enter(&kernel_mutex);

      ut_a(trx_sys->rseg_history_len >= n_removed_logs);
      trx_sys->rseg_history_len -= n_removed_logs;

      mutex_exit(&kernel_mutex);

      flst_truncate_end(rseg_hdr + TRX_RSEG_HISTORY, log_hdr + TRX_UNDO_HISTORY_NODE, n_removed_logs, &mtr);

      mutex_exit(&rseg->mutex);

      mtr.commit();

      return;
    }

    auto prev_hdr_addr = trx_purge_get_log_from_hist(flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));

    ++n_removed_logs;

    auto seg_hdr = undo_page + TRX_UNDO_SEG_HDR;

    if (mach_read_from_2(seg_hdr + TRX_UNDO_STATE) == TRX_UNDO_TO_PURGE && mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG) == 0) {

      /* We can free the whole log segment */

      mutex_exit(&rseg->mutex);

      mtr.commit();

      free_segment(rseg, hdr_addr, n_removed_logs);

      n_removed_logs = 0;

    } else {

      mutex_exit(&rseg->mutex);

      mtr.commit();
    }

    mtr.start();

    mutex_enter(&rseg->mutex);

    rseg_hdr = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);

    hdr_addr = prev_hdr_addr;
  }
}

void Purge_sys::truncate_history() noexcept {
  trx_id_t limit_trx_no;
  undo_no_t limit_undo_no;

  ut_ad(mutex_own(&m_mutex));

  trx_purge_arr_get_biggest(m_arr, &limit_trx_no, &limit_undo_no);

  if (limit_trx_no == 0) {

    limit_trx_no = m_purge_trx_no;
    limit_undo_no = m_purge_undo_no;
  }

  /* We play safe and set the truncate limit at most to the purge view
  low_limit number, though this is not necessary */

  if (limit_trx_no >= m_view->low_limit_no) {
    limit_trx_no = m_view->low_limit_no;
    limit_undo_no = 0;
  }

  ut_ad(limit_trx_no <= m_view->low_limit_no);

  for (auto rseg : trx_sys->rseg_list) {
    truncate_rseg_history(rseg, limit_trx_no, limit_undo_no);
  }
}

bool Purge_sys::truncate_if_arr_empty() noexcept {
  ut_ad(mutex_own(&m_mutex));

  if (m_arr->n_used == 0) {

    truncate_history();

    return true;

  } else  {

    return false;
  }
}

void Purge_sys::rseg_get_next_history_log(trx_rseg_t *rseg) noexcept {
  ut_ad(mutex_own(&m_mutex));

  mutex_enter(&rseg->mutex);

  ut_a(rseg->last_page_no != FIL_NULL);

  m_purge_trx_no = rseg->last_trx_no + 1;

  m_purge_undo_no = 0;

  m_next_stored = false;

  mtr_t mtr;

  mtr.start();

  auto undo_page = srv_undo->page_get_s_latched(rseg->space, rseg->last_page_no, &mtr);
  auto log_hdr = undo_page + rseg->last_offset;

  /* Increase the purge page count by one for every handled log */

  ++m_n_pages_handled;

  auto prev_log_addr = trx_purge_get_log_from_hist(flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));

  if (prev_log_addr.m_page_no == FIL_NULL) {
    /* No logs left in the history list */

    rseg->last_page_no = FIL_NULL;

    mutex_exit(&rseg->mutex);

    mtr.commit();

    mutex_enter(&kernel_mutex);

    /* Add debug code to track history list corruption reported
    on the MySQL mailing list on Nov 9, 2004. The fut0lst.c
    file-based list was corrupt. The prev node pointer was
    FIL_NULL, even though the list length was over 8 million nodes!
    We assume that purge truncates the history list in moderate
    size pieces, and if we here reach the head of the list, the
    list cannot be longer than 20 000 undo logs now. */

    if (trx_sys->rseg_history_len > 20000) {
        log_warn(std::format(
          "Purge reached the head of the history list, but its length is still"
          " reported as {}! Please submit a detailed bug report.",
          trx_sys->rseg_history_len
        ));
    }

    mutex_exit(&kernel_mutex);

    return;
  }

  mutex_exit(&rseg->mutex);

  mtr.commit();

  /* Read the trx number and del marks from the previous log header */
  mtr.start();

  log_hdr = srv_undo->page_get_s_latched(rseg->space, prev_log_addr.m_page_no, &mtr) + prev_log_addr.m_boffset;

  auto trx_no = mach_read_from_8(log_hdr + TRX_UNDO_TRX_NO);

  auto del_marks = mach_read_from_2(log_hdr + TRX_UNDO_DEL_MARKS);

  mtr.commit();

  mutex_enter(&rseg->mutex);

  rseg->last_page_no = prev_log_addr.m_page_no;
  rseg->last_offset = prev_log_addr.m_boffset;
  rseg->last_trx_no = trx_no;
  rseg->last_del_marks = del_marks;

  mutex_exit(&rseg->mutex);
}

void Purge_sys::choose_next_log() noexcept {
  ulint offset{};
  space_id_t space{};
  page_no_t page_no{};

  mtr_t mtr;

  ut_ad(mutex_own(&m_mutex));
  ut_ad(m_next_stored == false);

  trx_rseg_t *min_rseg{};
  auto min_trx_no = LSN_MAX;

  for (auto rseg : trx_sys->rseg_list) {
    mutex_enter(&rseg->mutex);

    if (rseg->last_page_no != FIL_NULL) {

      if (min_rseg == nullptr || min_trx_no > rseg->last_trx_no) {

        min_rseg = rseg;
        space = rseg->space;
        min_trx_no = rseg->last_trx_no;

	      /* We assume in purge of externally stored fields that space id == 0 */
        ut_a(space == SYS_TABLESPACE);

        page_no = rseg->last_page_no;
        offset = rseg->last_offset;
      }
    }

    mutex_exit(&rseg->mutex);
  }

  if (min_rseg == nullptr) {

    return;
  }

  trx_undo_rec_t *rec;

  mtr.start();

  if (!min_rseg->last_del_marks) {
    /* No need to purge this log */

    rec = &trx_purge_dummy_rec;

  } else {

    rec = srv_undo->get_first_rec(space, page_no, offset, RW_S_LATCH, &mtr);

    if (rec == nullptr) {
      /* Undo log empty */
      rec = &trx_purge_dummy_rec;
    }
  }

  m_next_stored = true;
  m_rseg = min_rseg;

  m_hdr_page_no = page_no;
  m_hdr_offset = offset;

  m_purge_trx_no = min_trx_no;

  if (rec == &trx_purge_dummy_rec) {

    m_purge_undo_no = 0;
    m_page_no = page_no;
    m_offset = 0;

  } else {

    m_purge_undo_no = trx_undo_rec_get_undo_no(rec);
    m_page_no = page_get_page_no(page_align(rec));
    m_offset = page_offset(rec);
  }

  mtr.commit();
}

trx_undo_rec_t *Purge_sys::get_next_rec(mem_heap_t *heap) noexcept {
  ulint type;
  page_t *page;
  ulint cmpl_info;
  trx_undo_rec_t *next_rec;

  ut_ad(mutex_own(&m_mutex));
  ut_ad(m_next_stored);

  auto space = m_rseg->space;
  auto page_no = m_page_no;
  auto offset = m_offset;

  if (offset == 0) {
    /* It is the dummy undo log record, which means that there is
    no need to purge this undo log */

    rseg_get_next_history_log(m_rseg);

    /* Look for the next undo log and record to purge */

    choose_next_log();

    return (&trx_purge_dummy_rec);
  }

  mtr_t mtr;

  mtr.start();

  auto undo_page = srv_undo->page_get_s_latched(space, page_no, &mtr);
  auto rec = undo_page + offset;
  auto rec2 = rec;

  for (;;) {
    /* Try first to find the next record which requires a purge
    operation from the same page of the same undo log */

    next_rec = trx_undo_page_get_next_rec(rec2, m_hdr_page_no, m_hdr_offset);

    if (next_rec == nullptr) {
      rec2 = srv_undo->get_next_rec(rec2, m_hdr_page_no, m_hdr_offset, &mtr);
      break;
    }

    rec2 = next_rec;

    type = trx_undo_rec_get_type(rec2);

    if (type == TRX_UNDO_DEL_MARK_REC) {

      break;
    }

    cmpl_info = trx_undo_rec_get_cmpl_info(rec2);

    if (trx_undo_rec_get_extern_storage(rec2)) {
      break;
    }

    if ((type == TRX_UNDO_UPD_EXIST_REC) && !(cmpl_info & UPD_NODE_NO_ORD_CHANGE)) {
      break;
    }
  }

  if (rec2 == nullptr) {
    mtr.commit();

    rseg_get_next_history_log(m_rseg);

    /* Look for the next undo log and record to purge */

    choose_next_log();

    mtr.start();

    undo_page = srv_undo->page_get_s_latched(space, page_no, &mtr);

    rec = undo_page + offset;
  } else {
    page = page_align(rec2);

    m_purge_undo_no = trx_undo_rec_get_undo_no(rec2);
    m_page_no = page_get_page_no(page);
    m_offset = rec2 - page;

    if (undo_page != page) {
      /* We advance to a new page of the undo log: */
      ++m_n_pages_handled;
    }
  }

  auto rec_copy = trx_undo_rec_copy(rec, heap);

  mtr.commit();

  return rec_copy;
}

Purge_sys::Purge_sys() noexcept {
  ut_ad(mutex_own(&kernel_mutex));

  m_state = PURGE_STATE_OFF;

  rw_lock_create(&m_latch, SYNC_PURGE_LATCH);

  mutex_create(&m_mutex, IF_DEBUG("Purge_sys::m_mutex",) IF_SYNC_DEBUG(SYNC_PURGE_SYS,) Source_location{});

  m_heap = mem_heap_create(256);

  m_arr = trx_undo_arr_create();

  m_sess = sess_open();

  m_trx = m_sess->trx;

  m_trx->m_is_purge = 1;

  ut_a(trx_start_low(m_trx, ULINT_UNDEFINED));

  m_query = graph_build();

  m_view = read_view_oldest_copy_or_open_new(0, m_heap);
}

Purge_sys::~Purge_sys() noexcept {
  ut_ad(!mutex_own(&kernel_mutex));

  que_graph_free(m_query);

  ut_a(m_sess->trx->m_is_purge);
  m_sess->trx->m_conc_state = TRX_NOT_STARTED;
  sess_close(m_sess);
  m_sess = nullptr;

  if (m_view != nullptr) {
    /* Because acquiring the kernel mutex is a pre-condition
    of read_view_close(). We don't really need it here. */
    mutex_enter(&kernel_mutex);

    read_view_close(m_view);
    m_view = nullptr;

    mutex_exit(&kernel_mutex);
  }

  trx_undo_arr_free(m_arr);

  rw_lock_free(&m_latch);
  mutex_free(&m_mutex);

  mem_heap_free(m_heap);
}

bool Purge_sys::update_undo_must_exist(trx_id_t trx_id) noexcept {
#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&m_latch, RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  return !read_view_sees_trx_id(m_view, trx_id);
}

void Purge_sys::add_update_undo_to_history(trx_t *trx, page_t *undo_page, mtr_t *mtr) noexcept {
  ulint hist_size;

  auto undo = trx->update_undo;
  auto rseg = undo->m_rseg;

  ut_ad(mutex_own(&rseg->mutex));

  auto rseg_header = trx_rsegf_get(rseg->space, rseg->page_no, mtr);
  auto undo_header = undo_page + undo->m_hdr_offset;
  auto seg_header = undo_page + TRX_UNDO_SEG_HDR;

  if (undo->m_state != TRX_UNDO_CACHED) {
    /* The undo log segment will not be reused */

    if (undo->m_id >= TRX_RSEG_N_SLOTS) {
      log_fatal("undo->m_id is n", undo->m_id);
    }

    trx_rsegf_set_nth_undo(rseg_header, undo->m_id, FIL_NULL, mtr);

    hist_size = mtr->read_ulint(rseg_header + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES);
    ut_a(undo->m_size == flst_get_len(seg_header + TRX_UNDO_PAGE_LIST, mtr));

    mlog_write_ulint(rseg_header + TRX_RSEG_HISTORY_SIZE, hist_size + undo->m_size, MLOG_4BYTES, mtr);
  }

  /* Add the log as the first in the history list */
  flst_add_first(rseg_header + TRX_RSEG_HISTORY, undo_header + TRX_UNDO_HISTORY_NODE, mtr);

  mutex_enter(&kernel_mutex);
  ++trx_sys->rseg_history_len;
  mutex_exit(&kernel_mutex);

  /* Write the trx number to the undo log header */
  mlog_write_uint64(undo_header + TRX_UNDO_TRX_NO, trx->m_no, mtr);

  /* Write information about delete markings to the undo log header */
  if (!undo->m_del_marks) {
    mlog_write_ulint(undo_header + TRX_UNDO_DEL_MARKS, false, MLOG_2BYTES, mtr);
  }

  if (rseg->last_page_no == FIL_NULL) {

    rseg->last_page_no = undo->m_hdr_page_no;
    rseg->last_offset = undo->m_hdr_offset;
    rseg->last_trx_no = trx->m_no;
    rseg->last_del_marks = undo->m_del_marks;
  }
}

void Purge_sys::rec_release(trx_undo_inf_t *cell) noexcept {
  mutex_enter(&m_mutex);

  arr_remove_info(cell);

  mutex_exit(&m_mutex);
}

ulint Purge_sys::run() noexcept {
  que_thr_t *thr;
  ulint old_pages_handled;

  mutex_enter(&m_mutex);

  if (m_trx->n_active_thrs > 0) {

    mutex_exit(&m_mutex);

    /* Should not happen */

    ut_error;

    return 0;
  }

  rw_lock_x_lock(&m_latch);

  mutex_enter(&kernel_mutex);

  /* Close and free the old purge view */

  read_view_close(m_view);
  m_view = nullptr;
  mem_heap_empty(m_heap);

  /* Determine how much data manipulation language (DML) statements
  need to be delayed in order to reduce the lagging of the purge
  thread. */
  srv_dml_needed_delay = 0; /* in microseconds; default: no delay */

  /* If we cannot advance the 'purge view' because of an old
  'consistent read view', then the DML statements cannot be delayed.
  Also, srv_max_purge_lag <= 0 means 'infinity'. */
  if (srv_max_purge_lag > 0 && !UT_LIST_GET_LAST(trx_sys->view_list)) {
    float ratio = (float)trx_sys->rseg_history_len / srv_max_purge_lag;
    if (ratio > (float)ULINT_MAX / 10000) {
      /* Avoid overflow: maximum delay is 4295 seconds */
      srv_dml_needed_delay = ULINT_MAX;
    } else if (ratio > 1) {
      /* If the history list length exceeds the
      innodb_max_purge_lag, the
      data manipulation statements are delayed
      by at least 5000 microseconds. */
      srv_dml_needed_delay = (ulint)((ratio - .5) * 10000);
    }
  }

  m_view = read_view_oldest_copy_or_open_new(0, m_heap);

  mutex_exit(&kernel_mutex);

  rw_lock_x_unlock(&m_latch);

  m_state = PURGE_STATE_ON;

  /* Handle at most 20 undo log pages in one purge batch */

  m_handle_limit = m_n_pages_handled + 20;

  old_pages_handled = m_n_pages_handled;

  mutex_exit(&m_mutex);

  mutex_enter(&kernel_mutex);

  thr = que_fork_start_command(m_query);

  ut_ad(thr);

  /*
    auto thr2 = que_fork_start_command(m_query);

    ut_ad(thr2);
  */

  mutex_exit(&kernel_mutex);

  /*	srv_que_task_enqueue(thr2); */

  if (srv_print_thread_releases) {

    log_info("Starting purge");
  }

  que_run_threads(thr);

  return m_n_pages_handled - old_pages_handled;
}

trx_undo_rec_t *Purge_sys::fetch_next_rec(roll_ptr_t *roll_ptr, trx_undo_inf_t **cell, mem_heap_t *heap) noexcept {
  trx_undo_rec_t *undo_rec;

  mutex_enter(&m_mutex);

  if (m_state == PURGE_STATE_OFF) {
    truncate_if_arr_empty();

    mutex_exit(&m_mutex);

    return nullptr;
  }

  if (!m_next_stored) {
    choose_next_log();

    if (!m_next_stored) {
      m_state = PURGE_STATE_OFF;

      truncate_if_arr_empty();

      mutex_exit(&m_mutex);

      return nullptr;
    }
  }

  if (m_n_pages_handled >= m_handle_limit) {

    m_state = PURGE_STATE_OFF;

    truncate_if_arr_empty();

    mutex_exit(&m_mutex);

    return nullptr;
  }

  if (m_purge_trx_no >= m_view->low_limit_no) {
    m_state = PURGE_STATE_OFF;

    truncate_if_arr_empty();

    mutex_exit(&m_mutex);

    return nullptr;
  }

  *roll_ptr = trx_undo_build_roll_ptr(false, m_rseg->id, m_page_no, m_offset);

  *cell = arr_store_info(m_purge_trx_no, m_purge_undo_no);

  ut_ad(m_purge_trx_no < m_view->low_limit_no);

  /* The following call will advance the stored values of purge_trx_no
  and purge_undo_no, therefore we had to store them first */

  undo_rec = get_next_rec(heap);

  mutex_exit(&m_mutex);

  return undo_rec;
}

std::string Purge_sys::to_string() noexcept {
  auto str = ::to_string(m_view);

  str += std::format("Purge trx n:o {} undo n:o {}\n", m_purge_trx_no, m_purge_undo_no);

  str += std::format(
    "Purge next stored {}, page_no {}, offset {}, Purge hdr_page_no {}, hdr_offset {}\n",
    m_next_stored,
    m_page_no,
    m_offset,
    m_hdr_page_no,
    m_hdr_offset
  );

  return str;
}
