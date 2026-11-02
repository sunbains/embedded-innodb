/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.
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

/** @file mtr/mtr0mtr.c
Mini-transaction buffer

Created 11/26/1995 Heikki Tuuri
*******************************************************/

#include "mtr0mtr.h"
#include "buf0buf.h"
#include "buf0types.h"
#include "log0log.h"
#include "log0recv.h"
#include "mtr0log.h"
#include "page0types.h"
#include "sync0rw.h"
#include "sync0sync.h"

/**
 * Releases the item in the slot given.
 *
 * @param[in,out] mtr           Release slot in this mtr.
 * @param[in] slot              Slot to release.
 * */
inline void mtr_memo_slot_release(System &sys, Mtr *mtr, MtrMemoSlot *slot) noexcept {
  auto object = slot->m_object;
  auto type = slot->m_type;

  if (likely(object != nullptr)) {
    if (type <= MTR_MEMO_BUF_FIX) {
      sys.m_buf_pool->release(static_cast<BufBlock *>(object), type, mtr);
    } else if (type == MTR_MEMO_S_LOCK) {
      rw_lock_s_unlock(static_cast<RwLock *>(object));
#ifndef UNIV_DEBUG
    } else {
      rw_lock_x_unlock(static_cast<RwLock *>(object));
    }
#else
    } else if (type == MTR_MEMO_X_LOCK) {
      rw_lock_x_unlock(static_cast<RwLock *>(object));
    } else {
      ut_ad(type == MTR_MEMO_MODIFY);
      ut_ad(mtr->memo_contains(object, MTR_MEMO_PAGE_X_FIX));
    }
#endif /* !UNIV_DEBUG */

    slot->m_object = nullptr;
  }
}

void Mtr::disable_redo_logging() noexcept {
  ut_ad(m_magic_n == MTR_MAGIC_N);
  m_log_mode = MTR_LOG_NONE;
}

static void mtr_memo_pop_all(Mtr *mtr) noexcept {
  ut_ad(mtr->m_magic_n == MTR_MAGIC_N);

  /* Currently only used in commit */
  ut_ad(mtr->m_state == MTR_COMMITTING);

  auto offset = mtr->m_memo.get_data_size();

  while (offset > 0) {
    offset -= sizeof(MtrMemoSlot);
    auto slot = static_cast<MtrMemoSlot *>(mtr->m_memo.get_element(offset));

    mtr_memo_slot_release(mtr, slot);
  }
}

/**
 * Writes the contents of a mini-transaction log, if any, to the database log.
 *
 * @param[in,out] mtr           Mini-transaction used for the write
 * @param[in,out] log           Log to write to
 * @param[in] recovery          recovery flag
 */
static void mtr_log_reserve_and_write(Mtr *mtr, Log *log, ulint recovery) noexcept {
  auto mlog = &mtr->m_log;
  auto first_data = mlog->get_data();

  if (mtr->m_n_log_recs > 1) {
    mlog_catenate_ulint(mtr, MLOG_MULTI_REC_END, MLOG_1BYTE);
  } else {
    *first_data = byte(ulint(*first_data) | MLOG_SINGLE_REC_FLAG);
  }

  if (mlog->single_block()) {

    log->acquire();

    mtr->m_end_lsn = log->reserve_and_write_fast(first_data, mlog->get_used(mlog->get_first_block()), &mtr->m_start_lsn);

    if (mtr->m_end_lsn != 0) {
      /* We were able to successfully write to the log. */
      return;
    }

    log->release();
  }

  auto data_size = mlog->get_data_size();

  /* Open the database log for log_write_low */
  mtr->m_start_lsn = log->reserve_and_open(data_size);

  if (mtr->m_log_mode == MTR_LOG_ALL) {

    auto block = mlog->get_first_block();

    while (block != nullptr) {
      log->write_low(mlog->get_data(block), mlog->get_used(block));
      block = mlog->get_next_block(block);
    }
  } else {
    ut_ad(mtr->m_log_mode == MTR_LOG_NONE);
    /* Do nothing */
  }

  mtr->m_end_lsn = log->close(IbRecovery(recovery));
}

void Mtr::commit(System &sys) noexcept {
  ut_ad(is_active());
  ut_ad(m_magic_n == MTR_MAGIC_N);

  m_state = MTR_COMMITTING;

  const auto write_log = m_modifications > 0 && m_n_log_recs > 0;

  if (write_log) {
    mtr_log_reserve_and_write(this, sys.m_log, sys.m_config.m_recv.m_force_recovery);
  }

  /* We first update the modification info to buffer pages, and only
  after that release the log mutex: this guarantees that when the log
  mutex is free, all buffer pages contain an up-to-date info of their
  modifications. This fact is used in making a checkpoint when we look
  at the oldest modification of any page in the buffer pool. It is also
  required when we insert modified buffer pages in to the flush list
  which must be sorted on oldest_modification. */

  mtr_memo_pop_all(this);

  if (write_log) {
    sys.m_log->release();
  }

  m_state = MTR_COMMITTED;
}

void Mtr::rollback_to_savepoint(ulint savepoint) noexcept {
  ut_ad(m_magic_n == MTR_MAGIC_N);
  ut_ad(is_active());

  auto offset = m_memo.get_data_size();
  ut_ad(offset >= savepoint);

  while (offset > savepoint) {
    offset -= sizeof(MtrMemoSlot);

    auto slot = static_cast<MtrMemoSlot *>(m_memo.get_element(offset));
    ut_ad(slot->m_type != MTR_MEMO_MODIFY);

    mtr_memo_slot_release(this, slot);
  }
}

void Mtr::memo_release(void *object, ulint type) noexcept {
  ut_ad(m_magic_n == MTR_MAGIC_N);
  ut_ad(is_active());

  auto offset = m_memo.get_data_size();

  while (offset > 0) {
    offset -= sizeof(MtrMemoSlot);

    auto slot = static_cast<MtrMemoSlot *>(m_memo.get_element(offset));

    if (object == slot->m_object && type == slot->m_type) {

      mtr_memo_slot_release(this, slot);

      break;
    }
  }
}

ulint Mtr::read_ulint(System &sys, const byte *ptr, ulint type) const noexcept {
  ut_ad(is_active());
  ut_ad(memo_contains_page(sys, ptr, MTR_MEMO_PAGE_S_FIX) || memo_contains_page(sys, ptr, MTR_MEMO_PAGE_X_FIX));
  if (type == MLOG_1BYTE) {
    return mach_read_from_1(ptr);
  } else if (type == MLOG_2BYTES) {
    return mach_read_from_2(ptr);
  } else {
    ut_ad(type == MLOG_4BYTES);
    return mach_read_from_4(ptr);
  }
}

uint64_t Mtr::read_uint64(System &sys, const byte *ptr) const noexcept {
  ut_ad(is_active());
  ut_ad(memo_contains_page(sys, ptr, MTR_MEMO_PAGE_S_FIX) || memo_contains_page(sys, ptr, MTR_MEMO_PAGE_X_FIX));
  return mach_read_from_8(ptr);
}

#ifdef UNIV_DEBUG
bool Mtr::memo_contains_page(System &sys, const byte *ptr, ulint type) const noexcept {
  return memo_contains(sys.m_buf_pool->block_align(ptr), type);
}

std::string Mtr::to_string() const noexcept {
  return std::format(
    "Mini-transaction handle: memo size {} bytes log size {} bytes", m_memo.get_data_size(), m_log.get_data_size()
  );
}
#endif /* UNIV_DEBUG */

void Mtr::release_block_at_savepoint(ulint savepoint, BufBlock *block) noexcept {
  ut_ad(is_active());
  ut_ad(m_magic_n == MTR_MAGIC_N);

  auto slot = static_cast<MtrMemoSlot *>(m_memo.get_element(savepoint));

  ut_a(slot->m_object == block);

  buf_page_release_latch(block, slot->m_type);

  block->acquire_mutex();
  block->fix_dec();
  block->release_mutex();

  slot->m_object = nullptr;
}
