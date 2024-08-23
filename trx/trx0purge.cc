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

/** The global data structure coordinating a purge */
trx_purge_t *purge_sys = nullptr;

/** A dummy undo record used as a return value when we have a whole undo log
which needs no purge */
trx_undo_rec_t trx_purge_dummy_rec;

void trx_purge_var_init() {
  purge_sys = nullptr;
  memset(&trx_purge_dummy_rec, 0x0, sizeof(trx_purge_dummy_rec));
}

/** Checks if trx_id is >= purge_view: then it is guaranteed that its update
undo log still exists in the system.
@return true if is sure that it is preserved, also if the function
returns false, it is possible that the undo log still exists in the
system */

bool trx_purge_update_undo_must_exist(trx_id_t trx_id) /** in: transaction id */
{
#ifdef UNIV_SYNC_DEBUG
  ut_ad(rw_lock_own(&(purge_sys->latch), RW_LOCK_SHARED));
#endif /* UNIV_SYNC_DEBUG */

  if (!read_view_sees_trx_id(purge_sys->view, trx_id)) {

    return (true);
  }

  return (false);
}

/** Stores info of an undo log record during a purge.
@return	pointer to the storage cell */
static trx_undo_inf_t *trx_purge_arr_store_info(
  trx_id_t trx_no, /** in: transaction number */
  undo_no_t undo_no
) /** in: undo number */
{
  auto arr = purge_sys->arr;

  for (ulint i = 0;; i++) {
    auto cell = trx_undo_arr_get_nth_info(arr, i);

    if (!(cell->in_use)) {
      /* Not in use, we may store here */
      cell->undo_no = undo_no;
      cell->trx_no = trx_no;
      cell->in_use = true;

      arr->n_used++;

      return (cell);
    }
  }
}

/** Removes info of an undo log record during a purge. */
inline void trx_purge_arr_remove_info(trx_undo_inf_t *cell) /** in: pointer to the storage cell */
{
  trx_undo_arr_t *arr;

  arr = purge_sys->arr;

  cell->in_use = false;

  ut_ad(arr->n_used > 0);

  arr->n_used--;
}

/** Gets the biggest pair of a trx number and an undo number in a purge array.  */
static void trx_purge_arr_get_biggest(
  trx_undo_arr_t *arr, /** in: purge array */
  trx_id_t *trx_no,    /** out: transaction number: 0 if array is empty */
  undo_no_t *undo_no
) /** out: undo number */
{
  trx_undo_inf_t *cell;
  trx_id_t pair_trx_no;
  undo_no_t pair_undo_no;
  ulint n_used;
  ulint i;
  ulint n;

  n = 0;
  n_used = arr->n_used;
  pair_trx_no = 0;
  pair_undo_no = 0;

  for (i = 0;; i++) {
    cell = trx_undo_arr_get_nth_info(arr, i);

    if (cell->in_use) {
      n++;

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

/** Builds a purge 'query' graph. The actual purge is performed by executing
this query graph.
@return	own: the query graph */
static que_t *trx_purge_graph_build(void) {
  mem_heap_t *heap;
  que_fork_t *fork;
  que_thr_t *thr;
  /*	que_thr_t*	thr2; */

  heap = mem_heap_create(512);
  fork = que_fork_create(nullptr, nullptr, QUE_FORK_PURGE, heap);
  fork->trx = purge_sys->trx;

  thr = que_thr_create(fork, heap);

  thr->child = row_purge_node_create(thr, heap);

  /*	thr2 = que_thr_create(fork, fork, heap);

  thr2->child = row_purge_node_create(fork, thr2, heap);	 */

  return (fork);
}

void trx_purge_sys_create() {
  ut_ad(mutex_own(&kernel_mutex));

  purge_sys = static_cast<trx_purge_t *>(mem_alloc(sizeof(trx_purge_t)));

  purge_sys->state = TRX_STOP_PURGE;

  purge_sys->n_pages_handled = 0;

  purge_sys->purge_trx_no = 0;
  purge_sys->purge_undo_no = 0;
  purge_sys->next_stored = false;

  rw_lock_create(&purge_sys->latch, SYNC_PURGE_LATCH);

  mutex_create(&purge_sys->mutex, IF_DEBUG("purge_sys_mutex",) IF_SYNC_DEBUG(SYNC_PURGE_SYS,) Source_location{});

  purge_sys->heap = mem_heap_create(256);

  purge_sys->arr = trx_undo_arr_create();

  purge_sys->sess = sess_open();

  purge_sys->trx = purge_sys->sess->trx;

  purge_sys->trx->m_is_purge = 1;

  ut_a(trx_start_low(purge_sys->trx, ULINT_UNDEFINED));

  purge_sys->query = trx_purge_graph_build();

  purge_sys->view = read_view_oldest_copy_or_open_new(0, purge_sys->heap);
}

void trx_purge_sys_close() {
  ut_ad(!mutex_own(&kernel_mutex));

  que_graph_free(purge_sys->query);

  ut_a(purge_sys->sess->trx->m_is_purge);
  purge_sys->sess->trx->m_conc_state = TRX_NOT_STARTED;
  sess_close(purge_sys->sess);
  purge_sys->sess = nullptr;

  if (purge_sys->view != nullptr) {
    /* Because acquiring the kernel mutex is a pre-condition
    of read_view_close(). We don't really need it here. */
    mutex_enter(&kernel_mutex);

    read_view_close(purge_sys->view);
    purge_sys->view = nullptr;

    mutex_exit(&kernel_mutex);
  }

  trx_undo_arr_free(purge_sys->arr);

  rw_lock_free(&purge_sys->latch);
  mutex_free(&purge_sys->mutex);

  mem_heap_free(purge_sys->heap);
  mem_free(purge_sys);

  purge_sys = nullptr;
}

void trx_purge_add_update_undo_to_history(trx_t *trx, page_t *undo_page, mtr_t *mtr) {
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

/** Frees an undo log segment which is in the history list. Cuts the end of the
history list at the youngest undo log in this segment. */
static void trx_purge_free_segment(
  trx_rseg_t *rseg,    /** in: rollback segment */
  fil_addr_t hdr_addr, /** in: the file address of log_hdr */
  ulint n_removed_logs
) /** in: count of how many undo logs we
                          will cut off from the end of the
                          history list */
{
  page_t *undo_page;
  trx_rsegf_t *rseg_hdr;
  trx_ulogf_t *log_hdr;
  trx_usegf_t *seg_hdr;
  bool freed;
  ulint seg_size;
  ulint hist_size;
  bool marked = false;
  mtr_t mtr;

  ut_ad(mutex_own(&(purge_sys->mutex)));
loop:
  mtr.start();
  mutex_enter(&(rseg->mutex));

  rseg_hdr = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);

  undo_page = srv_undo->page_get(rseg->space, hdr_addr.m_page_no, &mtr);
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

  freed = srv_fsp->fseg_free_step_not_header(seg_hdr + TRX_UNDO_FSEG_HEADER, &mtr);

  if (!freed) {
    mutex_exit(&rseg->mutex);
    mtr.commit();

    goto loop;
  }

  /* The page list may now be inconsistent, but the length field
  stored in the list base node tells us how big it was before we
  started the freeing. */

  seg_size = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST, &mtr);

  /* We may free the undo log segment header page; it must be freed
  within the same mtr as the undo log header is removed from the
  history list: otherwise, in case of a database crash, the segment
  could become inaccessible garbage in the file space. */

  flst_cut_end(rseg_hdr + TRX_RSEG_HISTORY, log_hdr + TRX_UNDO_HISTORY_NODE, n_removed_logs, &mtr);

  mutex_enter(&kernel_mutex);
  ut_ad(trx_sys->rseg_history_len >= n_removed_logs);
  trx_sys->rseg_history_len -= n_removed_logs;
  mutex_exit(&kernel_mutex);

  freed = false;

  while (!freed) {
    /* Here we assume that a file segment with just the header
    page can be freed in a few steps, so that the buffer pool
    is not flooded with bufferfixed pages: see the note in
    fsp0fsp.c. */

    freed = srv_fsp->fseg_free_step(seg_hdr + TRX_UNDO_FSEG_HEADER, &mtr);
  }

  hist_size = mtr.read_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES);
  ut_ad(hist_size >= seg_size);

  mlog_write_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, hist_size - seg_size, MLOG_4BYTES, &mtr);

  ut_ad(rseg->curr_size >= seg_size);

  rseg->curr_size -= seg_size;

  mutex_exit(&rseg->mutex);

  mtr.commit();
}

/** Removes unnecessary history data from a rollback segment. */
static void trx_purge_truncate_rseg_history(
  trx_rseg_t *rseg,      /** in: rollback segment */
  trx_id_t limit_trx_no, /** in: remove update undo logs whose
                             trx number is < limit_trx_no */
  undo_no_t limit_undo_no
) /** in: if transaction number is equal
                             to limit_trx_no, truncate undo records
                             with undo number < limit_undo_no */
{
  int cmp;
  page_t *undo_page;
  fil_addr_t prev_hdr_addr;
  trx_ulogf_t *log_hdr;
  trx_usegf_t *seg_hdr;
  ulint n_removed_logs = 0;

  ut_ad(mutex_own(&purge_sys->mutex));

  mtr_t mtr;

  mtr.start();

  mutex_enter(&rseg->mutex);

  auto rseg_hdr = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);

  auto hdr_addr = trx_purge_get_log_from_hist(flst_get_last(rseg_hdr + TRX_RSEG_HISTORY, &mtr));

loop:
  if (hdr_addr.m_page_no == FIL_NULL) {

    mutex_exit(&rseg->mutex);

    mtr.commit();

    return;
  }

  undo_page = srv_undo->page_get(rseg->space, hdr_addr.m_page_no, &mtr);

  log_hdr = undo_page + hdr_addr.m_boffset;

  cmp = mach_read_from_8(log_hdr + TRX_UNDO_TRX_NO) - limit_trx_no;

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

  prev_hdr_addr = trx_purge_get_log_from_hist(flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));

  ++n_removed_logs;

  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;

  if ((mach_read_from_2(seg_hdr + TRX_UNDO_STATE) == TRX_UNDO_TO_PURGE) && (mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG) == 0)) {

    /* We can free the whole log segment */

    mutex_exit(&rseg->mutex);

    mtr.commit();

    trx_purge_free_segment(rseg, hdr_addr, n_removed_logs);

    n_removed_logs = 0;
  } else {
    mutex_exit(&rseg->mutex);

    mtr.commit();
  }

  mtr.start();

  mutex_enter(&rseg->mutex);

  rseg_hdr = trx_rsegf_get(rseg->space, rseg->page_no, &mtr);

  hdr_addr = prev_hdr_addr;

  goto loop;
}

/** Removes unnecessary history data from rollback segments. NOTE that when this
function is called, the caller must not have any latches on undo log pages! */
static void trx_purge_truncate_history(void) {
  trx_rseg_t *rseg;
  trx_id_t limit_trx_no;
  undo_no_t limit_undo_no;

  ut_ad(mutex_own(&purge_sys->mutex));

  trx_purge_arr_get_biggest(purge_sys->arr, &limit_trx_no, &limit_undo_no);

  if (limit_trx_no == 0) {

    limit_trx_no = purge_sys->purge_trx_no;
    limit_undo_no = purge_sys->purge_undo_no;
  }

  /* We play safe and set the truncate limit at most to the purge view
  low_limit number, though this is not necessary */

  if (limit_trx_no >= purge_sys->view->low_limit_no) {
    limit_trx_no = purge_sys->view->low_limit_no;
    limit_undo_no = 0;
  }

  ut_ad(limit_trx_no <= purge_sys->view->low_limit_no);

  rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);

  while (rseg) {
    trx_purge_truncate_rseg_history(rseg, limit_trx_no, limit_undo_no);
    rseg = UT_LIST_GET_NEXT(rseg_list, rseg);
  }
}

/** Does a truncate if the purge array is empty. NOTE that when this function is
called, the caller must not have any latches on undo log pages!
@return	true if array empty */
inline bool trx_purge_truncate_if_arr_empty(void) {
  ut_ad(mutex_own(&purge_sys->mutex));

  if (purge_sys->arr->n_used == 0) {

    trx_purge_truncate_history();

    return (true);
  }

  return false;
}

/** Updates the last not yet purged history log info in rseg when we have purged
a whole undo log. Advances also purge_sys->purge_trx_no past the purged log. */
static void trx_purge_rseg_get_next_history_log(trx_rseg_t *rseg) /** in: rollback segment */
{
  bool del_marks;
  trx_id_t trx_no;
  fil_addr_t prev_log_addr;

  ut_ad(mutex_own(&purge_sys->mutex));

  mutex_enter(&rseg->mutex);

  ut_a(rseg->last_page_no != FIL_NULL);

  purge_sys->purge_trx_no = rseg->last_trx_no + 1;

  purge_sys->purge_undo_no = 0;

  purge_sys->next_stored = false;

  mtr_t mtr;

  mtr.start();

  auto undo_page = srv_undo->page_get_s_latched(rseg->space, rseg->last_page_no, &mtr);
  auto log_hdr = undo_page + rseg->last_offset;

  /* Increase the purge page count by one for every handled log */

  ++purge_sys->n_pages_handled;

  prev_log_addr = trx_purge_get_log_from_hist(flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));

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
      ut_print_timestamp(ib_stream);
      ib_logger(
        ib_stream,
        "  Warning: purge reached the"
        " head of the history list,\n"
        "but its length is still"
        " reported as %lu! Make a detailed bug\n"
        "report, and submit it.\n"
        "Check the InnoDB website for "
        "details\n",
        (ulong)trx_sys->rseg_history_len
      );
    }

    mutex_exit(&kernel_mutex);

    return;
  }

  mutex_exit(&(rseg->mutex));
  mtr.commit();

  /* Read the trx number and del marks from the previous log header */
  mtr.start();

  log_hdr = srv_undo->page_get_s_latched(rseg->space, prev_log_addr.m_page_no, &mtr) + prev_log_addr.m_boffset;

  trx_no = mach_read_from_8(log_hdr + TRX_UNDO_TRX_NO);

  del_marks = mach_read_from_2(log_hdr + TRX_UNDO_DEL_MARKS);

  mtr.commit();

  mutex_enter(&(rseg->mutex));

  rseg->last_page_no = prev_log_addr.m_page_no;
  rseg->last_offset = prev_log_addr.m_boffset;
  rseg->last_trx_no = trx_no;
  rseg->last_del_marks = del_marks;

  mutex_exit(&(rseg->mutex));
}

/** Chooses the next undo log to purge and updates the info in purge_sys. This
function is used to initialize purge_sys when the next record to purge is
not known, and also to update the purge system info on the next record when
purge has handled the whole undo log for a transaction. */
static void trx_purge_choose_next_log(void) {
  trx_undo_rec_t *rec;
  trx_rseg_t *rseg;
  trx_rseg_t *min_rseg;
  trx_id_t min_trx_no;
  space_id_t space {};
  page_no_t page_no{};
  ulint offset{};
  mtr_t mtr;

  ut_ad(mutex_own(&(purge_sys->mutex)));
  ut_ad(purge_sys->next_stored == false);

  rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);

  min_trx_no = LSN_MAX;

  min_rseg = nullptr;

  while (rseg != nullptr) {
    mutex_enter(&(rseg->mutex));

    if (rseg->last_page_no != FIL_NULL) {

      if (min_rseg == nullptr || min_trx_no > rseg->last_trx_no) {

        min_rseg = rseg;
        min_trx_no = rseg->last_trx_no;
        space = rseg->space;

	/* We assume in purge of externally stored fields that space id == 0 */
        ut_a(space == 0);

        page_no = rseg->last_page_no;
        offset = rseg->last_offset;
      }
    }

    mutex_exit(&(rseg->mutex));

    rseg = UT_LIST_GET_NEXT(rseg_list, rseg);
  }

  if (min_rseg == nullptr) {

    return;
  }

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

  purge_sys->next_stored = true;
  purge_sys->rseg = min_rseg;

  purge_sys->hdr_page_no = page_no;
  purge_sys->hdr_offset = offset;

  purge_sys->purge_trx_no = min_trx_no;

  if (rec == &trx_purge_dummy_rec) {

    purge_sys->purge_undo_no = 0;
    purge_sys->page_no = page_no;
    purge_sys->offset = 0;
  } else {
    purge_sys->purge_undo_no = trx_undo_rec_get_undo_no(rec);

    purge_sys->page_no = page_get_page_no(page_align(rec));
    purge_sys->offset = page_offset(rec);
  }

  mtr.commit();
}

/** Gets the next record to purge and updates the info in the purge system.
@return	copy of an undo log record or pointer to the dummy undo log record */
static trx_undo_rec_t *trx_purge_get_next_rec(mem_heap_t *heap) /** in: memory heap where copied */
{
  ulint type;
  page_t *page;
  ulint cmpl_info;
  trx_undo_rec_t *next_rec;

  ut_ad(mutex_own(&purge_sys->mutex));
  ut_ad(purge_sys->next_stored);

  auto space = purge_sys->rseg->space;
  auto page_no = purge_sys->page_no;
  auto offset = purge_sys->offset;

  if (offset == 0) {
    /* It is the dummy undo log record, which means that there is
    no need to purge this undo log */

    trx_purge_rseg_get_next_history_log(purge_sys->rseg);

    /* Look for the next undo log and record to purge */

    trx_purge_choose_next_log();

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

    next_rec = trx_undo_page_get_next_rec(rec2, purge_sys->hdr_page_no, purge_sys->hdr_offset);
    if (next_rec == nullptr) {
      rec2 = srv_undo->get_next_rec(rec2, purge_sys->hdr_page_no, purge_sys->hdr_offset, &mtr);
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

    trx_purge_rseg_get_next_history_log(purge_sys->rseg);

    /* Look for the next undo log and record to purge */

    trx_purge_choose_next_log();

    mtr.start();

    undo_page = srv_undo->page_get_s_latched(space, page_no, &mtr);

    rec = undo_page + offset;
  } else {
    page = page_align(rec2);

    purge_sys->purge_undo_no = trx_undo_rec_get_undo_no(rec2);
    purge_sys->page_no = page_get_page_no(page);
    purge_sys->offset = rec2 - page;

    if (undo_page != page) {
      /* We advance to a new page of the undo log: */
      purge_sys->n_pages_handled++;
    }
  }

  auto rec_copy = trx_undo_rec_copy(rec, heap);

  mtr.commit();

  return rec_copy;
}

trx_undo_rec_t *trx_purge_fetch_next_rec(roll_ptr_t *roll_ptr, trx_undo_inf_t **cell, mem_heap_t *heap) {
  trx_undo_rec_t *undo_rec;

  mutex_enter(&purge_sys->mutex);

  if (purge_sys->state == TRX_STOP_PURGE) {
    trx_purge_truncate_if_arr_empty();

    mutex_exit(&(purge_sys->mutex));

    return nullptr;
  }

  if (!purge_sys->next_stored) {
    trx_purge_choose_next_log();

    if (!purge_sys->next_stored) {
      purge_sys->state = TRX_STOP_PURGE;

      trx_purge_truncate_if_arr_empty();

      if (srv_print_thread_releases) {
        log_info(std::format(
          "Purge: No logs left in the history list; pages handled {}",
          purge_sys->n_pages_handled
        ));
      }

      mutex_exit(&purge_sys->mutex);

      return nullptr;
    }
  }

  if (purge_sys->n_pages_handled >= purge_sys->handle_limit) {

    purge_sys->state = TRX_STOP_PURGE;

    trx_purge_truncate_if_arr_empty();

    mutex_exit(&purge_sys->mutex);

    return nullptr;
  }

  if (purge_sys->purge_trx_no >= purge_sys->view->low_limit_no) {
    purge_sys->state = TRX_STOP_PURGE;

    trx_purge_truncate_if_arr_empty();

    mutex_exit(&purge_sys->mutex);

    return nullptr;
  }

  *roll_ptr = trx_undo_build_roll_ptr(false, (purge_sys->rseg)->id, purge_sys->page_no, purge_sys->offset);

  *cell = trx_purge_arr_store_info(purge_sys->purge_trx_no, purge_sys->purge_undo_no);

  ut_ad(purge_sys->purge_trx_no < purge_sys->view->low_limit_no);

  /* The following call will advance the stored values of purge_trx_no
  and purge_undo_no, therefore we had to store them first */

  undo_rec = trx_purge_get_next_rec(heap);

  mutex_exit(&purge_sys->mutex);

  return undo_rec;
}

void trx_purge_rec_release(trx_undo_inf_t *cell) {
  mutex_enter(&purge_sys->mutex);

  trx_purge_arr_remove_info(cell);

  mutex_exit(&purge_sys->mutex);
}

ulint trx_purge() {
  que_thr_t *thr;
  /*	que_thr_t*	thr2; */
  ulint old_pages_handled;

  mutex_enter(&purge_sys->mutex);

  if (purge_sys->trx->n_active_thrs > 0) {

    mutex_exit(&(purge_sys->mutex));

    /* Should not happen */

    ut_error;

    return 0;
  }

  rw_lock_x_lock(&purge_sys->latch);

  mutex_enter(&kernel_mutex);

  /* Close and free the old purge view */

  read_view_close(purge_sys->view);
  purge_sys->view = nullptr;
  mem_heap_empty(purge_sys->heap);

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

  purge_sys->view = read_view_oldest_copy_or_open_new(0, purge_sys->heap);

  mutex_exit(&kernel_mutex);

  rw_lock_x_unlock(&purge_sys->latch);

  purge_sys->state = TRX_PURGE_ON;

  /* Handle at most 20 undo log pages in one purge batch */

  purge_sys->handle_limit = purge_sys->n_pages_handled + 20;

  old_pages_handled = purge_sys->n_pages_handled;

  mutex_exit(&purge_sys->mutex);

  mutex_enter(&kernel_mutex);

  thr = que_fork_start_command(purge_sys->query);

  ut_ad(thr);

  /*	thr2 = que_fork_start_command(purge_sys->query);

  ut_ad(thr2); */

  mutex_exit(&kernel_mutex);

  /*	srv_que_task_enqueue(thr2); */

  if (srv_print_thread_releases) {

    log_info("Starting purge");
  }

  que_run_threads(thr);

  if (srv_print_thread_releases) {

    log_info("Purge ends; pages handled ", purge_sys->n_pages_handled);
  }

  return purge_sys->n_pages_handled - old_pages_handled;
}

/** Prints information of the purge system to ib_stream. */

void trx_purge_sys_print(void) {
  log_info("Purge system view:");
  read_view_print(purge_sys->view);

  ib_logger(
    ib_stream,
    "Purge trx n:o %lu undo n:o %lu\n",
    TRX_ID_PREP_PRINTF(purge_sys->purge_trx_no),
    TRX_ID_PREP_PRINTF(purge_sys->purge_undo_no)
  );
  ib_logger(
    ib_stream,
    "Purge next stored %lu, page_no %lu, offset %lu,\n"
    "Purge hdr_page_no %lu, hdr_offset %lu\n",
    (ulong)purge_sys->next_stored,
    (ulong)purge_sys->page_no,
    (ulong)purge_sys->offset,
    (ulong)purge_sys->hdr_page_no,
    (ulong)purge_sys->hdr_offset
  );
}
