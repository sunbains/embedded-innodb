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

/** @file include/mtr0mtr.h
Mini-transaction buffer

Created 11/26/1995 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0types.h"
#include "dyn0dyn.h"
#include "mem0mem.h"
#include "mtr0types.h"
#include "page0types.h"
#include "sync0rw.h"
#include "ut0byte.h"
#include "mach0data.h"
#include "sync0rw.h"
#include "sync0sync.h"

/* @} */


/**
 * @brief Commits a mini-transaction.
 *
 * @param mtr The mini-transaction to commit.
 */
void mtr_commit(mtr_t *mtr);

/**
 * @brief Releases the latches stored in an mtr memo down to a savepoint.
 * 
 * @note The mtr must not have made changes to buffer pages after the savepoint,
 * as these can be handled only by mtr_commit.
 *
 * @param mtr The mtr.
 * @param savepoint The savepoint.
 */
void mtr_rollback_to_savepoint(mtr_t *mtr, ulint savepoint);

/**
 * @brief Reads 1 - 4 bytes from a file page buffered in the buffer pool.
 *
 * @param ptr Pointer from where to read.
 * @param type Type of the value to read (MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES).
 * @param mtr Mini-transaction handle.
 * @return The value read.
 */
ulint mtr_read_ulint(const byte *ptr, ulint type, mtr_t *mtr);

/**
 * @brief Reads 8 bytes from a file page buffered in the buffer pool.
 *
 * @param ptr Pointer from where to read.
 * @param mtr Mini-transaction handle.
 * @return The value read.
 */
uint64_t mtr_read_uint64(const byte *ptr, mtr_t *mtr);


/** This macro locks an rw-lock in s-mode. */
#define mtr_s_lock(B, MTR) mtr_s_lock_func((B), __FILE__, __LINE__, (MTR))

/** This macro locks an rw-lock in x-mode. */
#define mtr_x_lock(B, MTR) mtr_x_lock_func((B), __FILE__, __LINE__, (MTR))

/**
 * @brief Releases an object in the memo stack.
 *
 * @param mtr The mini-transaction handle.
 * @param object The object to release.
 * @param type The object type: MTR_MEMO_S_LOCK, ...
 */
void mtr_memo_release(mtr_t *mtr, void *object, ulint type);

#ifdef UNIV_DEBUG
/** Checks if memo contains the given page.
@return	true if contains */
bool mtr_memo_contains_page(mtr_t *mtr,      /*!< in: mtr */
                            const byte *ptr, /*!< in: pointer to buffer frame */
                            ulint type);     /*!< in: type of object */

/** Prints info of an mtr handle. */
void mtr_print(mtr_t *mtr); /*!< in: mtr */
#endif                      /* UNIV_DEBUG */

/*######################################################################*/

/**
 * @brief Starts a mini-transaction and creates a mini-transaction handle and a buffer in the memory buffer given by the caller.
 * 
 * @param mtr Memory buffer for the mtr buffer.
 * @return mtr buffer which also acts as the mtr handle.
 */
inline mtr_t *mtr_start(mtr_t *mtr) {
  dyn_array_create(&(mtr->memo));
  dyn_array_create(&(mtr->log));

  mtr->log_mode = MTR_LOG_ALL;
  mtr->modifications = false;
  mtr->n_log_recs = 0;

  ut_d(mtr->state = MTR_ACTIVE);
  ut_d(mtr->magic_n = MTR_MAGIC_N);

  return mtr;
}

/**
 * @brief Pushes an object to an mtr memo stack.
 * 
 * @param mtr The mtr.
 * @param object The object.
 * @param type The object type: MTR_MEMO_S_LOCK, ...
 */
inline void mtr_memo_push(mtr_t *mtr, void *object, ulint type) {
  ut_ad(type >= MTR_MEMO_PAGE_S_FIX);
  ut_ad(type <= MTR_MEMO_X_LOCK);
  ut_ad(mtr->magic_n == MTR_MAGIC_N);
  ut_ad(mtr->state == MTR_ACTIVE);

  auto memo = &mtr->memo;
  auto slot = reinterpret_cast<mtr_memo_slot_t *>(dyn_array_push(memo, sizeof(mtr_memo_slot_t)));

  slot->object = object;
  slot->type = type;
}

/**
 * @brief Sets and returns a savepoint in mtr.
 * 
 * @param mtr The mtr.
 * @return Savepoint.
 */
inline ulint mtr_set_savepoint(mtr_t *mtr) {
  ut_ad(mtr->magic_n == MTR_MAGIC_N);
  ut_ad(mtr->state == MTR_ACTIVE);

  auto memo = &mtr->memo;

  return dyn_array_get_data_size(memo);
}

/**
 * @brief Releases the (index tree) s-latch stored in an mtr memo after a savepoint.
 * 
 * @param mtr The mtr.
 * @param savepoint The savepoint.
 * @param lock The latch to release.
 */
inline void mtr_release_s_latch_at_savepoint(mtr_t *mtr, ulint savepoint, rw_lock_t *lock) {
  ut_ad(mtr->magic_n == MTR_MAGIC_N);
  ut_ad(mtr->state == MTR_ACTIVE);

  auto memo = &mtr->memo;

  ut_ad(dyn_array_get_data_size(memo) > savepoint);

  auto slot = (mtr_memo_slot_t *)dyn_array_get_element(memo, savepoint);

  ut_ad(slot->object == lock);
  ut_ad(slot->type == MTR_MEMO_S_LOCK);

  rw_lock_s_unlock(lock);

  slot->object = NULL;
}

#ifdef UNIV_DEBUG
/**
 * @brief Checks if the mtr memo contains the given item.
 * 
 * @param mtr The mtr.
 * @param object The object to search.
 * @param type The type of object.
 * @return True if the memo contains the item, false otherwise.
 */
inline bool mtr_memo_contains(mtr_t *mtr, const void *object, ulint type) {
  ut_ad(mtr->magic_n == MTR_MAGIC_N);
  ut_ad(mtr->state == MTR_ACTIVE || mtr->state == MTR_COMMITTING);

  auto memo = &mtr->memo;
  auto offset = dyn_array_get_data_size(memo);

  while (offset > 0) {
    offset -= sizeof(mtr_memo_slot_t);

    auto slot = (mtr_memo_slot_t *)dyn_array_get_element(memo, offset);

    if (object == slot->object && type == slot->type) {
      return true;
    }
  }

  return false;
}
#endif /* UNIV_DEBUG */

/**
 * @brief Gets the logging mode of a mini-transaction.
 * 
 * @param mtr The mtr.
 * @return Logging mode: MTR_LOG_NONE, ...
 */
inline ulint mtr_get_log_mode(mtr_t *mtr) {
  ut_ad(mtr);
  ut_ad(mtr->log_mode >= MTR_LOG_ALL);
  ut_ad(mtr->log_mode <= MTR_LOG_SHORT_INSERTS);

  return (mtr->log_mode);
}

/**
 * @brief Changes the logging mode of a mini-transaction.
 * 
 * @param mtr The mtr.
 * @param mode Logging mode: MTR_LOG_NONE, ...
 * @return Old mode.
 */
inline ulint mtr_set_log_mode(mtr_t *mtr, ulint mode) {
  ut_ad(mode >= MTR_LOG_ALL);
  ut_ad(mode <= MTR_LOG_SHORT_INSERTS);

  auto old_mode = mtr->log_mode;

  if ((mode == MTR_LOG_SHORT_INSERTS) && (old_mode == MTR_LOG_NONE)) {
    /* Do nothing */
  } else {
    mtr->log_mode = mode;
  }

  ut_ad(old_mode >= MTR_LOG_ALL);
  ut_ad(old_mode <= MTR_LOG_SHORT_INSERTS);

  return old_mode;
}

/**
 * @brief Locks a lock in s-mode.
 * 
 * @param lock The rw-lock.
 * @param file The file name.
 * @param line The line number.
 * @param mtr The mtr.
 */
inline void mtr_s_lock_func(rw_lock_t *lock, const char *file, ulint line, mtr_t *mtr) {
  rw_lock_s_lock_func(lock, 0, file, line);

  mtr_memo_push(mtr, lock, MTR_MEMO_S_LOCK);
}

/**
 * @brief Locks a lock in x-mode.
 * 
 * @param lock The rw-lock.
 * @param file The file name.
 * @param line The line number.
 * @param mtr The mtr.
 */
inline void mtr_x_lock_func(rw_lock_t *lock, const char *file, ulint line, mtr_t *mtr) {
  rw_lock_x_lock_func(lock, 0, file, line);

  mtr_memo_push(mtr, lock, MTR_MEMO_X_LOCK);
}
