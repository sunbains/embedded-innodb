/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
Copyright (c) 2025 Sunny Bains. All rights reserved.

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

/** @file lock/lock0validator.cc
Lock system validation and debugging

*******************************************************/

#include "lock0validator.h"
#include "buf0buf.h"
#include "dict0dict.h"
#include "lock0lock.h"
#include "row0row.h"
#include "srv0srv.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "ut0dbg.h"

// Node getters for ut_list iteration - these are defined in lock0lock.cc
struct TableLockGetNode {
  static const UtListNode<Lock> &get_node(const Lock &lock) { return lock.m_table.m_locks; }
};

struct RecLockGetNode {
  static const UtListNode<Lock> &get_node(const Lock &lock) { return lock.m_rec.m_rec_locks; }
};

/**
 * @brief Validate the transaction state.
 *
 * @param[in] trx The transaction to validate.
 */
static inline void validate_trx_state(const Trx *trx) noexcept {
  switch (trx->m_conc_state) {
    case TRX_ACTIVE:
    case TRX_PREPARED:
      break;
    case TRX_COMMITTED_IN_MEMORY:
      ut_error;
    default:
      ut_error;
  }
}

LockValidator::LockValidator(LockSys *lock_sys) noexcept : m_lock_sys(lock_sys) {}

#ifdef UNIV_DEBUG
bool LockValidator::validate(System &sys) noexcept {
  mutex_enter(&sys.m_trx_sys->m_mutex);

  auto trx_sys = sys.m_trx_sys.get();

  for (auto trx : trx_sys->m_trx_list) {

    for (auto lock : trx->m_trx_locks) {

      if (lock->type() == LOCK_TABLE) {
        (void)table_queue_validate(sys, lock->m_table.m_table);
      }
    }
  }

  /* Iterate over all record locks and validate each page */
  for (auto &[page_id, rec_locks] : m_lock_sys->m_rec_locks) {

    mutex_exit(&sys.m_trx_sys->m_mutex);

    (void)rec_validate_page(sys, page_id);

    mutex_enter(&sys.m_trx_sys->m_mutex);
  }

  mutex_exit(&sys.m_trx_sys->m_mutex);

  return true;
}

bool LockValidator::rec_validate_page(System &sys, PageId page_id) noexcept {
  ut_ad(!mutex_own(&sys.m_trx_sys->m_mutex));

  Mtr mtr;
  MemHeap *heap{};
  bool clean_up_called{};

  auto clean_up = [&](Mtr &mtr, MemHeap *heap) -> bool {
    if (heap != nullptr) {
      mem_heap_free(heap);
    }

    mtr.commit(sys);

    clean_up_called = true;

    return true;
  };

  mtr.start();

  auto buf_pool = sys.m_buf_pool;

  BufPool::Request req{.m_page_id = page_id, .m_mode = BUF_GET_NO_LATCH, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr};

  auto block = buf_pool->get(req, nullptr);

  if (unlikely(!block)) {
    /* The page has been freed */
    return clean_up(mtr, heap);
  }

  auto page = block->m_frame;
  const auto n_slots = page_dir_get_n_slots(page);

  for (ulint i{}; i < n_slots; ++i) {
    const auto dir_slot = page_dir_get_nth_slot(page, i);
    (void)page_dir_slot_get_rec(dir_slot);

    for (auto rec_cursor = page_get_infimum_rec(page); rec_cursor != page_get_supremum_rec(page);
         rec_cursor = page_rec_get_next(rec_cursor)) {

      if (page_rec_is_user_rec(rec_cursor)) {

        /* We cannot get offsets for the record here because
        we do not know the index. */

        mutex_enter(&sys.m_trx_sys->m_mutex);

        if (auto it = m_lock_sys->m_rec_locks.find(page_id); it != m_lock_sys->m_rec_locks.end()) {
          auto heap_no = page_rec_get_heap_no(rec_cursor);

          for (auto lock : it->second) {

            ut_a(sys.m_trx_sys->in_trx_list(lock->m_trx));

            validate_trx_state(lock->m_trx);

#ifdef UNIV_SYNC_DEBUG
            /* Only validate the record queues when this thread is not
            holding a space->latch.  Deadlocks are possible due to
            latching order violation when UNIV_DEBUG is defined while
            UNIV_SYNC_DEBUG is not. */
            const FilSpace *space = fil_space_get_by_id(space_id);
            if (space != nullptr && mutex_own(&space->m_latch)) {
              mutex_exit(&m_lock_sys->m_trx_sys->m_mutex);
              return clean_up(mtr, heap);
            }
#endif /* UNIV_SYNC_DEBUG */

            if (!lock->rec_is_nth_bit_set(heap_no)) {
              continue;
            }

            Index *index = lock->rec_index();

            if (heap == nullptr) {
              heap = mem_heap_create(256);
            }

            std::array<ulint, REC_OFFS_NORMAL_SIZE> offsets_{};
            RecOffs offsets(offsets_.data());

            {
              PhyRec record{index, Rec(rec_cursor)};
              offsets = record.get_all_col_offsets(offsets, &heap, Current_location());
            }

            mutex_exit(&sys.m_trx_sys->m_mutex);

            /* If this thread is holding the file space latch (FilSpace::latch), the following
              check WILL break the latching order and may cause a deadlock of threads. */

            (void)rec_queue_validate(block, Rec(rec_cursor), index, offsets);

            mutex_enter(&sys.m_trx_sys->m_mutex);

            if (clean_up_called) {
              return true;
            }
          }
        }

        mutex_exit(&sys.m_trx_sys->m_mutex);
      }
    }
  }

  return clean_up(mtr, heap);
}

bool LockValidator::table_queue_validate(System &sys, Table *table) noexcept {
  ut_ad(mutex_own(&sys.m_trx_sys->m_mutex));

  for (auto lock : table->m_locks) {
    validate_trx_state(lock->m_trx);

    if (!lock->is_waiting()) {

      ut_ad(!m_lock_sys->table_has_to_wait_in_queue(lock));
    }
  }

  return true;
}

bool LockValidator::rec_queue_validate(System &sys, const BufBlock *block, const Rec rec, const Index *index, const ulint *offsets) noexcept {
  ut_ad(block->get_frame() == rec.page_align());
  ut_ad(rec.offs_validate(index, offsets));

  mutex_enter(&sys.m_trx_sys->m_mutex);

  auto page_id = block->get_page_id();
  auto heap_no = page_rec_get_heap_no(rec);

  if (auto it = m_lock_sys->m_rec_locks.find(page_id); it != m_lock_sys->m_rec_locks.end()) {
    for (auto lock : it->second) {

      ut_ad(lock->type() == LOCK_REC);

      if (!lock->rec_is_nth_bit_set(heap_no)) {
        continue;
      }

      ut_ad(lock->m_trx->m_trx_sys == sys.m_trx_sys);

      validate_trx_state(lock->m_trx);
      ut_ad(sys.m_trx_sys->in_trx_list(lock->m_trx));

      if (lock->is_waiting()) {

        ut_ad(m_lock_sys->rec_has_to_wait_in_queue(it->second, lock, heap_no));
      }
    }

    for (auto lock : it->second) {

      ut_ad(lock->type() == LOCK_REC);

      if (!lock->rec_is_nth_bit_set(heap_no)) {
        continue;
      }

      validate_trx_state(lock->m_trx);
      ut_ad(sys.m_trx_sys->in_trx_list(lock->m_trx));

      if (index != nullptr) {

        ut_a(lock->rec_index() == index);
      }
    }
  }

  mutex_exit(&sys.m_trx_sys->m_mutex);

  return true;
}

const Lock *LockValidator::rec_other_has_expl_req(System &sys, 
  PageId page_id, LockMode mode, ulint gap, ulint wait, ulint heap_no, const Trx *trx
) const noexcept {
  ut_ad(mutex_own(&sys.m_trx_sys->m_mutex));
  ut_ad(mode == LOCK_X || mode == LOCK_S);
  ut_ad(gap == 0 || gap == LOCK_GAP);
  ut_ad(wait == 0 || wait == LOCK_WAIT);

  if (auto it = m_lock_sys->m_rec_locks.find(page_id); it != m_lock_sys->m_rec_locks.end()) {
    for (auto lock : it->second) {

      if (trx && lock->m_trx == trx) {
        continue;
      }

      if (lock->rec_is_nth_bit_set(heap_no) && (gap == 0 || !(lock->precise_mode() & LOCK_REC_NOT_GAP)) &&
          (wait == 0 || lock->is_waiting()) && mode == lock->mode()) {

        return lock;
      }
    }
  }

  return nullptr;
}

const Lock *LockValidator::rec_exists(System &sys, const Rec_locks &rec_locks, ulint heap_no) const noexcept {
  ut_ad(mutex_own(&sys.m_trx_sys->m_mutex));

  for (auto lock : rec_locks) {
    if (lock->rec_is_nth_bit_set(heap_no)) {
      return lock;
    }
  }

  return nullptr;
}

#endif /* UNIV_DEBUG */

bool LockValidator::print_info_summary(bool nowait) const noexcept {
  // TODO: Implement this
  return true;
}

void LockValidator::print_info_all_transactions() noexcept {
  // TODO: Implement this
}
