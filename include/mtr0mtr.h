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

/** @file include/mtr0mtr.h
Mini-transaction buffer

Created 11/26/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0types.h"
#include "dyn0dyn.h"
#include "mach0data.h"
#include "mem0mem.h"
#include "mtr0types.h"
#include "page0types.h"
#include "sync0rw.h"
#include "sync0sync.h"
#include "ut0byte.h"


/* Mini-transaction handle and buffer */
struct mtr_t {
  /**
   * @brief Starts a mini-transaction and creates a mini-transaction handle and a buffer in the memory buffer given by the caller.
   * 
   * @return mtr buffer which also acts as the mtr handle.
   */
  inline mtr_t *start() noexcept {
    dyn_array_create(&m_memo);
    dyn_array_create(&m_log);

    m_log_mode = MTR_LOG_ALL;
    m_modifications = false;
    m_n_log_recs = 0;
    m_state = MTR_ACTIVE;

    ut_d(m_magic_n = MTR_MAGIC_N);

    return this;
  }

  /**
   * @brief Releases the latches stored in an mtr memo down to a savepoint.
   * 
   * @note The mtr must not have made changes to buffer pages after the savepoint,
   * as these can be handled only by mtr_commit.
   *
   * @param[in] savepoint       The savepoint.
   */
  void rollback_to_savepoint(ulint savepoint) noexcept;

  /**
   * @brief Reads 1 - 4 bytes from a file page buffered in the buffer pool.
   *
   * @param[in,out] ptr         Pointer from where to read.
   * @param[in] type            Type of the value to read (MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES).
   * 
   * @return The value read.
   */
  [[nodiscard]] ulint read_ulint(const byte *ptr, ulint type) const noexcept;

  /**
   * @brief Reads 8 bytes from a file page buffered in the buffer pool.
   *
   * @param[in] ptr             Pointer from where to read.
   * 
   * @return The value read.
   */
  [[nodiscard]] uint64_t read_uint64(const byte *ptr) const noexcept;

  /**
   * Releases the block in an mtr memo after a savepoint.
   * 
   * @param[in] savepoint       The savepoint.
   * @param[in] block           The block to release.
   */
  void release_block_at_savepoint(ulint savepoint, buf_block_t *block) noexcept;

  /**
   * @brief Releases an object in the memo stack.
   *
   * @param[in] object          The object to release.
   * @param[in] type            The object type: MTR_MEMO_S_LOCK, ...
   */
  void memo_release(void *object, ulint type) noexcept;

  /**
   * @brief Pushes an object to an mtr memo stack.
   * 
   * @param[in] object          The object.
   * @param[in] type            The object type: MTR_MEMO_S_LOCK, ...
   */
  void memo_push(void *object, mtr_memo_type_t type) noexcept { 
    ut_ad(type >= MTR_MEMO_PAGE_S_FIX);
    ut_ad(type <= MTR_MEMO_X_LOCK);
    ut_ad(m_magic_n == MTR_MAGIC_N);
    ut_ad(m_state == MTR_ACTIVE);

    auto slot = reinterpret_cast<mtr_memo_slot_t *>(dyn_array_push(&m_memo, sizeof(mtr_memo_slot_t)));

    slot->m_type = type;
    slot->m_object = object;
  }

  /**
   * @brief Sets and returns a savepoint in mtr.
   * 
   * @return Savepoint.
   */
  [[nodiscard]] ulint set_savepoint() noexcept {
    ut_ad(m_magic_n == MTR_MAGIC_N);
    ut_ad(m_state == MTR_ACTIVE);

    // FIXME: This can become expensive multi-block mtrs
    return dyn_array_get_data_size(&m_memo);
  }

  /**
   * @brief Gets a savepoint.
   * 
   * @return Savepoint.
   */
  [[nodiscard]] ulint get_savepoint() noexcept {
    return set_savepoint();
  }

  /**
   * @brief Releases the (index tree) s-latch stored in an mtr memo after a savepoint.
   * 
   * @param[in] savepoint       The savepoint.
   * @param[in] lock            The latch to release.
   */
  void release_s_latch_at_savepoint(ulint savepoint, rw_lock_t *lock) const noexcept {
    ut_ad(m_magic_n == MTR_MAGIC_N);
    ut_ad(m_state == MTR_ACTIVE);

    ut_ad(dyn_array_get_data_size(&m_memo) > savepoint);

    auto slot = static_cast<mtr_memo_slot_t *>(dyn_array_get_element(&m_memo, savepoint));

    ut_ad(slot->m_object == lock);
    ut_ad(slot->m_type == MTR_MEMO_S_LOCK);

    rw_lock_s_unlock(lock);

    slot->m_object = nullptr;
  }
  /**
   * @brief Gets the logging mode of a mini-transaction.
   * 
   * @param mtr The mtr.
   * @return Logging mode: MTR_LOG_NONE, ...
   */
  [[nodiscard]] mtr_log_mode_t get_log_mode() const noexcept {
    ut_ad(m_log_mode >= MTR_LOG_ALL);
    ut_ad(m_log_mode <= MTR_LOG_SHORT_INSERTS);

    return m_log_mode;
  }

  /**
   * @brief Changes the logging mode of a mini-transaction.
   * 
   * @param mtr The mtr.
   * @param mode Logging mode: MTR_LOG_NONE, ...
   * @return Old mode.
   */
  [[nodiscard]] mtr_log_mode_t set_log_mode(mtr_log_mode_t mode) noexcept {
    ut_ad(mode >= MTR_LOG_ALL);
    ut_ad(mode <= MTR_LOG_SHORT_INSERTS);

    auto old_mode = m_log_mode;

    if (mode == MTR_LOG_SHORT_INSERTS && old_mode == MTR_LOG_NONE) {
      /* Do nothing */
    } else {
      m_log_mode = mode;
    }

    ut_ad(old_mode >= MTR_LOG_ALL);
    ut_ad(old_mode <= MTR_LOG_SHORT_INSERTS);

    return old_mode;
  }

  /**
   * @brief Locks a lock in s-mode.
   * 
   * @param[in] lock            The rw-lock.
   * @param[in] file            The file name.
   * @param[in] line            The line number.
   */
  void s_lock_func(rw_lock_t *lock, const char *file, ulint line) {
    rw_lock_s_lock_func(lock, 0, file, line);

    memo_push(lock, MTR_MEMO_S_LOCK);
  }

  /**
   * @brief Locks a lock in x-mode.
   * 
   * @param[in] lock            The rw-lock.
   * @param[in] file            The file name.
   * @param[in] line The line number.
   */
  inline void x_lock_func(rw_lock_t *lock, const char *file, ulint line) {
    rw_lock_x_lock_func(lock, 0, file, line);

    memo_push(lock, MTR_MEMO_X_LOCK);
  }

  /* @return true if the mtr is active. */
  [[nodiscard]] inline bool is_active() const noexcept {
    return m_state == MTR_ACTIVE;
  }

  #ifdef UNIV_DEBUG
  /**
   * Checks if memo contains the given page.
   * 
   * @param[in] ptr             Pointer to buffer frame.
   * @param[in] type            Type of object.
   * 
   * @return	true if contains
   */
  bool memo_contains_page(const byte *ptr, ulint type) const noexcept;

  /**
   * Prints info of an mtr handle.
   */
  std::string to_string() const noexcept;

  /**
   * @brief Checks if the mtr memo contains the given item.
   * 
   * @param[in] object          The object to search.
   * @param[in] type            The type of object.
   * 
   * @return True if the memo contains the item, false otherwise.
   */
  [[nodiscard]] bool memo_contains(const void *object, ulint type) const noexcept {
    ut_ad(m_magic_n == MTR_MAGIC_N);
    ut_ad(m_state == MTR_ACTIVE || m_state == MTR_COMMITTING);

    auto offset = dyn_array_get_data_size(&m_memo);

    while (offset > 0) {
      offset -= sizeof(mtr_memo_slot_t);

      auto slot = static_cast<mtr_memo_slot_t *>(dyn_array_get_element(&m_memo, offset));

      if (object == slot->m_object && type == slot->m_type) {
        return true;
      }
    }

    return false;
  }
  #endif /* UNIV_DEBUG */

  /**
   * @brief Commits a mini-transaction.
   *
   * @param mtr The mini-transaction to commit.
   */
  void commit() noexcept;

  /**
   * Disable redo loggin.
   */
  void disable_redo_logging() noexcept;

  /** Mini-transaction state. */
  mtr_state_t m_state{MTR_UNDEFINED};

  /** True if the mtr made modifications to buffer pool pages */
  bool m_modifications;

  /** Specifies which operations should be logged. */
  mtr_log_mode_t m_log_mode{MTR_LOG_ALL};

  /** Count of how many page initial log records have been written to the mtr log */
  uint32_t m_n_log_recs;

  /** Memo stack for locks etc. */
  dyn_array_t m_memo;

  /** Mini-transaction log */
  dyn_array_t m_log;

  /** Start lsn of the possible log entry for this mtr */
  lsn_t m_start_lsn;

  /** End lsn of the possible log entry for this mtr */
  lsn_t m_end_lsn;

  /** For debugging. */
  ut_d(ulint m_magic_n;)
};

/** This macro locks an rw-lock in s-mode. */
#define mtr_s_lock(B, MTR) (MTR)->s_lock_func((B), __FILE__, __LINE__)

/** This macro locks an rw-lock in x-mode. */
#define mtr_x_lock(B, MTR) (MTR)->x_lock_func((B), __FILE__, __LINE__)
