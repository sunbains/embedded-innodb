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

/** @file trx/trx0rseg.c
Rollback segment

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#include "trx0rseg.h"

#include "fut0lst.h"
#include "srv0srv.h"
#include "trx0purge.h"
#include "trx0undo.h"

trx_rseg_t *trx_rseg_get_on_id(ulint id) {
  auto rseg = UT_LIST_GET_FIRST(srv_trx_sys->m_rseg_list);
  ut_ad(rseg != nullptr);

  while (rseg->id != id) {
    rseg = UT_LIST_GET_NEXT(rseg_list, rseg);
    ut_ad(rseg != nullptr);
  }

  return rseg;
}

page_no_t trx_rseg_header_create(space_id_t space, ulint max_size, ulint *slot_no, mtr_t *mtr) {
  ut_ad(mutex_own(&kernel_mutex));
  ut_ad(mtr->memo_contains(srv_fil->space_get_latch(space), MTR_MEMO_X_LOCK));

  auto sys_header = srv_trx_sys->read_header(mtr);

  *slot_no = srv_trx_sys->frseg_find_free(mtr);

  if (*slot_no == ULINT_UNDEFINED) {

    return FIL_NULL;
  }

  /* Allocate a new file segment for the rollback segment */
  auto block = srv_fsp->fseg_create(space, 0, TRX_RSEG + TRX_RSEG_FSEG_HEADER, mtr);

  if (block == nullptr) {
    /* No space left */

    return FIL_NULL;
  }

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_RSEG_HEADER_NEW));

  auto page_no = block->get_page_no();

  /* Get the rollback segment file page */
  auto rsegf = trx_rsegf_get_new(space, page_no, mtr);

  /* Initialize max size field */
  mlog_write_ulint(rsegf + TRX_RSEG_MAX_SIZE, max_size, MLOG_4BYTES, mtr);

  /* Initialize the history list */

  mlog_write_ulint(rsegf + TRX_RSEG_HISTORY_SIZE, 0, MLOG_4BYTES, mtr);
  flst_init(rsegf + TRX_RSEG_HISTORY, mtr);

  /* Reset the undo log slots */
  for (ulint i = 0; i < TRX_RSEG_N_SLOTS; ++i) {

    trx_rsegf_set_nth_undo(rsegf, i, FIL_NULL, mtr);
  }

  /* Add the rollback segment info to the free slot in the trx system
  header */

  srv_trx_sys->frseg_set_space(sys_header, *slot_no, space, mtr);
  srv_trx_sys->frseg_set_page_no(sys_header, *slot_no, page_no, mtr);

  return page_no;
}

void trx_rseg_mem_free(trx_rseg_t *rseg) {
  mutex_free(&rseg->mutex);

  /* There can't be any active transactions. */
  ut_a(UT_LIST_GET_LEN(rseg->update_undo_list) == 0);
  ut_a(UT_LIST_GET_LEN(rseg->insert_undo_list) == 0);

  auto undo = UT_LIST_GET_FIRST(rseg->update_undo_cached);

  while (undo != nullptr) {
    auto prev_undo = undo;

    undo = UT_LIST_GET_NEXT(m_undo_list, undo);
    UT_LIST_REMOVE(rseg->update_undo_cached, prev_undo);

    Undo::delete_undo(prev_undo);
  }

  undo = UT_LIST_GET_FIRST(rseg->insert_undo_cached);

  while (undo != nullptr) {
    auto prev_undo = undo;

    undo = UT_LIST_GET_NEXT(m_undo_list, undo);
    UT_LIST_REMOVE(rseg->insert_undo_cached, prev_undo);

    Undo::delete_undo(prev_undo);
  }

  srv_trx_sys->set_nth_rseg(rseg->id, nullptr);

  mem_free(rseg);
}

/** Creates and initializes a rollback segment object. The values for the
fields are read from the header. The object is inserted to the rseg
list of the trx system object and a pointer is inserted in the rseg
array in the trx system object.
@return	own: rollback segment object */
static trx_rseg_t *trx_rseg_mem_create(
  ib_recovery_t recovery, /*!< in: recovery flag */
  ulint id,               /*!< in: rollback segment id */
  ulint space,            /*!< in: space where the segment placed */
  ulint page_no,          /*!< in: page number of the segment header */
  mtr_t *mtr
) /*!< in: mtr */
{
  ut_ad(mutex_own(&kernel_mutex));

  auto rseg = static_cast<trx_rseg_t *>(mem_alloc(sizeof(trx_rseg_t)));

  rseg->id = id;
  rseg->space = space;
  rseg->page_no = page_no;

  mutex_create(&rseg->mutex, IF_DEBUG("rseg_mutex", ) IF_SYNC_DEBUG(SYNC_RSEG, ) Current_location());

  UT_LIST_ADD_LAST(srv_trx_sys->m_rseg_list, rseg);

  srv_trx_sys->set_nth_rseg(id, rseg);

  auto rseg_header = trx_rsegf_get_new(space, page_no, mtr);

  rseg->max_size = mtr->read_ulint(rseg_header + TRX_RSEG_MAX_SIZE, MLOG_4BYTES);

  /* Initialize the undo log lists according to the rseg header */

  auto sum_of_undo_sizes = srv_undo->lists_init(recovery, rseg);

  rseg->curr_size = mtr->read_ulint(rseg_header + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES) + 1 + sum_of_undo_sizes;

  auto len = flst_get_len(rseg_header + TRX_RSEG_HISTORY, mtr);

  if (len > 0) {
    srv_trx_sys->m_rseg_history_len += len;

    auto node_addr = trx_purge_get_log_from_hist(flst_get_last(rseg_header + TRX_RSEG_HISTORY, mtr));

    rseg->last_page_no = node_addr.m_page_no;
    rseg->last_offset = node_addr.m_boffset;

    auto undo_log_hdr = srv_undo->page_get(rseg->space, node_addr.m_page_no, mtr) + node_addr.m_boffset;

    rseg->last_trx_no = mtr->read_uint64(undo_log_hdr + TRX_UNDO_TRX_NO);
    rseg->last_del_marks = mtr->read_ulint(undo_log_hdr + TRX_UNDO_DEL_MARKS, MLOG_2BYTES);
  } else {
    rseg->last_page_no = FIL_NULL;
  }

  return rseg;
}

void trx_rseg_list_and_array_init(ib_recovery_t recovery, trx_sysf_t *sys_header, mtr_t *mtr) {
  UT_LIST_INIT(srv_trx_sys->m_rseg_list);

  srv_trx_sys->m_rseg_history_len = 0;

  for (ulint i{}; i < TRX_SYS_N_RSEGS; ++i) {

    const auto page_no = srv_trx_sys->frseg_get_page_no(sys_header, i, mtr);

    if (page_no == FIL_NULL) {

      srv_trx_sys->set_nth_rseg(i, nullptr);

    } else {
      const auto space = srv_trx_sys->frseg_get_space(sys_header, i, mtr);

      trx_rseg_mem_create(recovery, i, space, page_no, mtr);
    }
  }
}
