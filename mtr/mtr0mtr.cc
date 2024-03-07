/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

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

/** @file mtr/mtr0mtr.c
Mini-transaction buffer

Created 11/26/1995 Heikki Tuuri
*******************************************************/

#include "mtr0mtr.h"

#include "buf0buf.h"
#include "log0log.h"
#include "mtr0log.h"
#include "page0types.h"
#include "log0recv.h"

/** Releases the item in the slot given.
@param[in,out] mtr              Release slot in this mtr.
@param[in] slot                 Slot to release. */
inline void mtr_memo_slot_release(mtr_t *mtr, mtr_memo_slot_t *slot) {
  ut_ad(mtr && slot);

  auto object = slot->object;
  auto type = slot->type;

  if (likely(object != NULL)) {
    if (type <= MTR_MEMO_BUF_FIX) {
      buf_page_release((buf_block_t *)object, type, mtr);
    } else if (type == MTR_MEMO_S_LOCK) {
      rw_lock_s_unlock((rw_lock_t *)object);
#ifdef UNIV_DEBUG
    } else if (type != MTR_MEMO_X_LOCK) {
      ut_ad(type == MTR_MEMO_MODIFY);
      ut_ad(mtr_memo_contains(mtr, object, MTR_MEMO_PAGE_X_FIX));
#endif /* UNIV_DEBUG */
    } else {
      rw_lock_x_unlock((rw_lock_t *)object);
    }
  }

  slot->object = NULL;
}

void mtr_memo_pop_all(mtr_t *mtr) {
  ut_ad(mtr);
  ut_ad(mtr->magic_n == MTR_MAGIC_N);

  /* Currently only used in commit */
  ut_ad(mtr->state == MTR_COMMITTING);

  auto memo = &(mtr->memo);
  auto offset = dyn_array_get_data_size(memo);

  while (offset > 0) {
    offset -= sizeof(mtr_memo_slot_t);
    auto slot =
        static_cast<mtr_memo_slot_t *>(dyn_array_get_element(memo, offset));

    mtr_memo_slot_release(mtr, slot);
  }
}

/** Writes the contents of a mini-transaction log, if any, to the database log.
@param[in, out] mtr             Mini-transaction used for the write
@param[in] recovery             recovery flag */
static void mtr_log_reserve_and_write(mtr_t *mtr, ulint recovery) {
  dyn_block_t *block;
  ulint data_size;

  ut_ad(mtr);

  auto mlog = &mtr->log;
  auto first_data = dyn_block_get_data(mlog);

  if (mtr->n_log_recs > 1) {
    mlog_catenate_ulint(mtr, MLOG_MULTI_REC_END, MLOG_1BYTE);
  } else {
    *first_data = (byte)((ulint)*first_data | MLOG_SINGLE_REC_FLAG);
  }

  if (mlog->heap == NULL) {

    log_acquire();

    mtr->end_lsn = log_reserve_and_write_fast(
        first_data, dyn_block_get_used(mlog), &mtr->start_lsn);

    if (mtr->end_lsn != 0) {
      /* We were able to successfully write to the log. */
      return;
    }

    log_release();
  }

  data_size = dyn_array_get_data_size(mlog);

  /* Open the database log for log_write_low */
  mtr->start_lsn = log_reserve_and_open(data_size);

  if (mtr->log_mode == MTR_LOG_ALL) {

    block = mlog;

    while (block != NULL) {
      log_write_low(dyn_block_get_data(block), dyn_block_get_used(block));
      block = dyn_array_get_next_block(mlog, block);
    }
  } else {
    ut_ad(mtr->log_mode == MTR_LOG_NONE);
    /* Do nothing */
  }

  mtr->end_lsn = log_close(ib_recovery_t(recovery));
}

void mtr_commit(mtr_t *mtr) {
  ut_ad(mtr);
  ut_ad(mtr->magic_n == MTR_MAGIC_N);
  ut_ad(mtr->state == MTR_ACTIVE);
  ut_d(mtr->state = MTR_COMMITTING);

  /* This is a dirty read, for debugging. */
  auto write_log = mtr->modifications && mtr->n_log_recs;

  if (write_log) {
    mtr_log_reserve_and_write(mtr, srv_force_recovery);
  }

  /* We first update the modification info to buffer pages, and only
  after that release the log mutex: this guarantees that when the log
  mutex is free, all buffer pages contain an up-to-date info of their
  modifications. This fact is used in making a checkpoint when we look
  at the oldest modification of any page in the buffer pool. It is also
  required when we insert modified buffer pages in to the flush list
  which must be sorted on oldest_modification. */

  mtr_memo_pop_all(mtr);

  if (write_log) {
    log_release();
  }

  ut_d(mtr->state = MTR_COMMITTED);
  dyn_array_free(&(mtr->memo));
  dyn_array_free(&(mtr->log));
}

void mtr_rollback_to_savepoint(mtr_t *mtr, ulint savepoint) {
  mtr_memo_slot_t *slot;
  dyn_array_t *memo;
  ulint offset;

  ut_ad(mtr);
  ut_ad(mtr->magic_n == MTR_MAGIC_N);
  ut_ad(mtr->state == MTR_ACTIVE);

  memo = &(mtr->memo);

  offset = dyn_array_get_data_size(memo);
  ut_ad(offset >= savepoint);

  while (offset > savepoint) {
    offset -= sizeof(mtr_memo_slot_t);

    slot = static_cast<mtr_memo_slot_t *>(dyn_array_get_element(memo, offset));

    ut_ad(slot->type != MTR_MEMO_MODIFY);
    mtr_memo_slot_release(mtr, slot);
  }
}

void mtr_memo_release(mtr_t *mtr, void *object, ulint type) {
  ut_ad(mtr);
  ut_ad(mtr->magic_n == MTR_MAGIC_N);
  ut_ad(mtr->state == MTR_ACTIVE);

  auto memo = &mtr->memo;
  auto offset = dyn_array_get_data_size(memo);

  while (offset > 0) {
    offset -= sizeof(mtr_memo_slot_t);

    auto slot =
        static_cast<mtr_memo_slot_t *>(dyn_array_get_element(memo, offset));

    if ((object == slot->object) && (type == slot->type)) {

      mtr_memo_slot_release(mtr, slot);

      break;
    }
  }
}

ulint mtr_read_ulint(const byte *ptr, ulint type,
                     mtr_t *mtr __attribute__((unused))) {
  ut_ad(mtr->state == MTR_ACTIVE);
  ut_ad(mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_S_FIX) ||
        mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_X_FIX));
  if (type == MLOG_1BYTE) {
    return mach_read_from_1(ptr);
  } else if (type == MLOG_2BYTES) {
    return mach_read_from_2(ptr);
  } else {
    ut_ad(type == MLOG_4BYTES);
    return mach_read_from_4(ptr);
  }
}

uint64_t mtr_read_uint64(const byte *ptr, mtr_t *mtr __attribute__((unused))) {
  ut_ad(mtr->state == MTR_ACTIVE);
  ut_ad(mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_S_FIX) ||
        mtr_memo_contains_page(mtr, ptr, MTR_MEMO_PAGE_X_FIX));
  return mach_read_from_8(ptr);
}

#ifdef UNIV_DEBUG
bool mtr_memo_contains_page(mtr_t *mtr, const byte *ptr, ulint type) {
  return mtr_memo_contains(mtr, buf_block_align(ptr), type);
}

void mtr_print(mtr_t *mtr) {
  ib_logger(ib_stream,
            "Mini-transaction handle: memo size %lu bytes"
            " log size %lu bytes\n",
            (ulong)dyn_array_get_data_size(&(mtr->memo)),
            (ulong)dyn_array_get_data_size(&(mtr->log)));
}
#endif /* UNIV_DEBUG */
