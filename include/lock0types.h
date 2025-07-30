/*****************************************************************************
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

/**************************************************/ /**
 @file include/lock0types.h
 The transaction lock system global types

 Created 5/7/1996 Heikki Tuuri
 *******************************************************/

#pragma once

#include "innodb0types.h"

#include "mem0types.h"
#include "ut0lst.h"

struct Lock;
struct Trx;
struct Buf_pool;
struct Table;
struct Index;

/** Lock types */
/* @{ */

using lock_type_t = uint16_t;

/** Mask used to extract mode from the type_mode field in a lock */
constexpr lock_type_t LOCK_MODE_MASK = 0xFUL;

/** Table lock */
constexpr lock_type_t LOCK_TABLE = 16;

/** Record lock. */
constexpr lock_type_t LOCK_REC = 32;

/** Mask used to extract lock type from the type_mode field in a lock */
constexpr lock_type_t LOCK_TYPE_MASK = 0xF0UL;

static_assert(!(LOCK_MODE_MASK & LOCK_TYPE_MASK), "error LOCK_MODE_MASK & LOCK_TYPE_MASK");

/** Waiting lock flag; when set, it means that the lock has not
yet been granted, it is just waiting for its turn in the wait queue */
constexpr lock_type_t LOCK_WAIT = 256;

/* Precise modes */

/** This flag denotes an ordinary next-key lock in contrast to
LOCK_GAP or LOCK_REC_NOT_GAP */
constexpr lock_type_t LOCK_ORDINARY = 0;

/** When this bit is set, it means that the lock holds only on the
gap before the record; for instance, an x-lock on the gap does not
give permission to modify the record on which the bit is set; locks
of this type are created when records are removed from the index
chain of records */
constexpr lock_type_t LOCK_GAP = 512;

/** This bit means that the lock is only on the index record and does
NOT block inserts to the gap before the index record; this is used in
the case when we retrieve a record with a unique key, and is also used
in locking plain SELECTs (not part of UPDATE or DELETE) when the user
has set the READ COMMITTED isolation level */
constexpr lock_type_t LOCK_REC_NOT_GAP = 1024;

/** This bit is set when we place a waiting gap type record lock request
in order to let an insert of an index record to wait until there are no
conflicting locks by other transactions on the gap; note that this flag
remains set when the waiting lock is granted, or if the lock is inherited
to a neighboring record */
constexpr lock_type_t LOCK_INSERT_INTENTION = 2048;

static_assert(
  !((LOCK_WAIT | LOCK_GAP | LOCK_REC_NOT_GAP | LOCK_INSERT_INTENTION) & LOCK_MODE_MASK),
  "Lock modes should be independent bits and be maskable by LOCK_MODE_MASK"
);

static_assert(
  !((LOCK_WAIT | LOCK_GAP | LOCK_REC_NOT_GAP | LOCK_INSERT_INTENTION) & LOCK_TYPE_MASK),
  "Lock types should be independent bits and be maskable by LOCK_TYPE_MASK"
);

/* @} */

/* Basic lock modes */
enum Lock_mode : lock_type_t {
  /* Intention shared */
  LOCK_IS = 0,

  /* Intention exclusive */
  LOCK_IX,

  /* Shared */
  LOCK_S,

  /* Exclusive */
  LOCK_X,

  /* This is used elsewhere to note consistent read */
  LOCK_NONE,

  /* Number of lock modes */
  LOCK_NUM = LOCK_NONE
};

using Lock_mode_type = typename std::underlying_type<Lock_mode>::type;

/** A table lock */
struct Table_lock {
  /** Database table in dictionary cache */
  Table *m_table;

  /** List of locks on the same table */
  UT_LIST_NODE_T(Lock) m_locks;
};

/** Play it safe. */
static_assert(std::is_standard_layout<Table_lock>::value, "Table_lock must have a standard layout");

/** Record lock for a page */
struct Rec_lock {
  using List_node = UT_LIST_NODE_T(Lock);

  /** Index record lock */
  const Index *m_index;

  /** Page ID. */
  Page_id m_page_id;

  /** List of the record locks on the same page */
  List_node m_rec_locks;
};

/** We need this to read beyond Rec_lock. */
static_assert(std::is_standard_layout<Rec_lock>::value, "Rec_lock must have a standard layout");

/** Lock struct */
struct Lock {
  /**
   * @brief Gets the id of the transaction owning a lock.
   *
   * This function retrieves the transaction id of the transaction that owns the specified lock.
   *
   * @return The transaction id of the transaction owning the lock.
   */
  [[nodiscard]] inline trx_id_t trx_id() const noexcept;

  /**
   * @brief Gets the type of the lock.
   *
   * This method retrieves the type of the lock by applying a mask
   * to the lock's type and mode field.
   *
   * @return LOCK_TABLE or LOCK_REC
   */
  [[nodiscard]] inline ulint type() const noexcept {
    const auto type = m_type_mode & LOCK_TYPE_MASK;
    ut_ad(type == LOCK_REC || type == LOCK_TABLE);
    return type;
  }

  /**
   * @brief Gets the mode of a lock.
   *
   * This function retrieves the mode of the specified lock by
   * extracting the mode bits from the lock's type mode.
   *
   * @return The mode of the lock as a Lock_mode enum.
   */
  [[nodiscard]] inline Lock_mode mode() const noexcept { return static_cast<Lock_mode>(m_type_mode & LOCK_MODE_MASK); }

  /**
   * @brief Gets the precise type mode of a lock.
   *
   * This function retrieves the precise type mode of the specified lock by
   * extracting the type and mode bits from the lock's type mode.
   *
   * @return The type mode of the lock as a lock_type_t.
   */
  [[nodiscard]] inline lock_type_t precise_mode() const noexcept { return m_type_mode; }

  /**
   * @brief Sets the wait flag of a lock and the back pointer in trx to lock.
   *
   * This function sets the wait flag in the lock's type mode and updates
   * the transaction's wait_lock pointer to point to the specified lock.
   *
   * @param[in] trx The transaction that is waiting for the lock.
   */
  inline void set_trx_wait(Trx *trx) noexcept;

  /**
   * @brief Gets the wait flag of a lock.
   *
   * This function checks if the specified lock has the wait flag set.
   *
   * @return true if the lock is waiting, false otherwise.
   */
  [[nodiscard]] inline bool is_waiting() const noexcept { return (m_type_mode & LOCK_WAIT) != 0; }

  /**
   * @brief Gets the gap flag of a record lock.
   *
   * This function checks if the gap flag is set for the specified record lock.
   *
   * @return true if the gap flag is set, false otherwise.
   */
  [[nodiscard]] bool rec_is_gap() const noexcept {
    ut_ad(type() == LOCK_REC);
    return (m_type_mode & LOCK_GAP) != 0;
  }

  /**
   * @brief Checks if the record lock is not a gap lock.
   *
   * This function verifies if the LOCK_REC_NOT_GAP flag is set for the specified record lock.
   *
   * @return true if the LOCK_REC_NOT_GAP flag is set, false otherwise.
   */
  [[nodiscard]] bool rec_is_not_gap() const noexcept {
    ut_ad(type() == LOCK_REC);
    return (m_type_mode & LOCK_REC_NOT_GAP) != 0;
  }

  /**
   * @brief Checks if the record lock has the insert intention flag set.
   *
   * This function verifies if the LOCK_INSERT_INTENTION flag is set for the specified record lock.
   *
   * @return true if the LOCK_INSERT_INTENTION flag is set, false otherwise.
   */
  [[nodiscard]] bool rec_is_insert_intention() const noexcept {
    ut_ad(type() == LOCK_REC);
    return (m_type_mode & LOCK_INSERT_INTENTION) != 0;
  }

  /**
   * @brief Checks if a lock request for a new lock has to wait for another lock.
   *
   * This function determines whether a new lock request by a transaction (trx)
   * has to wait for an existing lock (this) to be removed. The decision is
   * based on the type and mode of the new lock, the characteristics of the
   * existing lock, and whether the lock is on the 'supremum' record of an
   * index page.
   *
   * @param[in] trx                  The transaction requesting the new lock.
   * @param[in] type_mode            The requested precise mode of the new lock to set:
   *                                 LOCK_S or LOCK_X, possibly ORed with LOCK_GAP,
   *                                 LOCK_REC_NOT_GAP, or LOCK_INSERT_INTENTION.
   * @param[in] lock_is_on_supremum  True if the lock is being set on the
   *                                 'supremum' record of an index page,
   *                                 indicating that the lock request is
   *                                 really for a 'gap' type lock.
   *
   * @return true if the new lock has to wait for lock to be removed, false otherwise.
   */
  [[nodiscard]] inline bool rec_blocks(const Trx *trx, Lock_mode_type type_mode, bool lock_is_on_supremum) const noexcept;

  /**
   * @brief Checks if a lock request lock1 has to wait for request lock2.
   *
   * This function determines whether the lock request represented by lock1
   * must wait for the lock request represented by lock2 to be removed.
   *
   * @param[in] lock  Another lock. It is assumed that this lock has a lock bit
   *                  set on the same record as in lock1 if the locks are record locks.
   * @param[in] heap_no The heap number of the record if the lock is a record lock, otherwise ULINT_UNDEFINED.
   *
   * @return true if lock1 has to wait for lock2 to be removed.
   */
  [[nodiscard]] bool has_to_wait_for(const Lock *lock, ulint heap_no) const noexcept;

  /**
   * @brief Resets the wait flag of a lock and the back pointer in the transaction.
   *
   * This function sets the back pointer to a waiting lock request in the transaction to nullptr
   * and resets the wait bit in the lock's type mode.
   *
   * @param[in] lock The lock whose wait flag and transaction back pointer are to be reset.
   */
  inline void reset() noexcept;

  /**
   * @brief Sets the nth bit of a record lock to true.
   *
   * This function sets the bit at the specified index in the bitmap
   * of a record lock to true. The bitmap represents the lock status
   * of individual records.
   *
   * @param[in] i The index of the bit to set.
   */
  inline void rec_set_nth_bit(ulint i) noexcept {
    ut_ad(type() == LOCK_REC);
    ut_ad(i < m_n_bits);

    const auto byte_index = i / 8;
    const auto bit_index = i % 8;

    (reinterpret_cast<byte *>(&this[1]))[byte_index] |= 1 << bit_index;
  }

  /**
   * @brief Gets the number of bits in a record lock bitmap.
   *
   * This function retrieves the total number of bits in the bitmap
   * of a given record lock. The bitmap represents the lock status
   * of individual records.
   *
   * @return The number of bits in the record lock bitmap.
   */
  [[nodiscard]] inline ulint rec_get_n_bits() const noexcept {
    ut_ad(type() == LOCK_REC);
    return m_n_bits;
  }

  /**
   * @brief Looks for a set bit in a record lock bitmap.
   *
   * This function searches through the bitmap of a record lock to find a set bit.
   * If no set bit is found, it returns ULINT_UNDEFINED.
   *
   * @return The bit index, which is the heap number of the record, or ULINT_UNDEFINED if none is found.
   */
  [[nodiscard]] ulint rec_find_set_bit() const noexcept {
    ut_ad(type() == LOCK_REC);

    for (ulint i{}; i < rec_get_n_bits(); ++i) {

      if (rec_is_nth_bit_set(i)) {

        return i;
      }
    }

    return ULINT_UNDEFINED;
  }

  /**
   * @brief Gets the nth bit of a record lock.
   *
   * This function retrieves the value of the nth bit in the bitmap of a record lock.
   * It checks if the bit at the specified index is set.
   *
   * @param[in] lock The record lock.
   * @param[in] i The index of the bit to check.
   *
   * @return true if the bit is set, false if the bit is not set or if the index is out of bounds.
   */
  [[nodiscard]] inline bool rec_is_nth_bit_set(ulint i) const noexcept {
    ut_ad(type() == LOCK_REC);

    if (i >= m_n_bits) {
      return false;
    }

    const auto byte_index = i / 8;
    const auto bit_index = i % 8;

    return (1 & (reinterpret_cast<const byte *>(&this[1]))[byte_index] >> bit_index) != 0;
  }

  /**
   * @brief Resets the nth bit of a record lock.
   *
   * This function clears the bit at the specified index in the bitmap
   * of a record lock. The bitmap represents the lock status of individual records.
   *
   * @param[in] i The index of the bit to reset.
   */
  inline void rec_reset_nth_bit(ulint i) noexcept {
    ut_ad(type() == LOCK_REC);
    ut_ad(i < m_n_bits);

    const auto byte_index = i / 8;
    const auto bit_index = i % 8;

    reinterpret_cast<byte *>(&this[1])[byte_index] &= ~(1 << bit_index);
  }

  /**
   * @brief Resets the record lock bitmap to zero.
   *
   * This function resets the bitmap of a record lock to zero. It does not
   * modify the wait_lock pointer in the transaction. This function is used
   * during the creation and resetting of lock objects.
   *
   */
  void rec_bitmap_reset() noexcept {
    ut_ad(type() == LOCK_REC);

    /* Reset to zero the bitmap which resides immediately after the lock struct */

    const auto n_bytes = rec_get_n_bits() / 8;

    ut_ad((rec_get_n_bits() % 8) == 0);

    std::memset(reinterpret_cast<byte *>(&this[1]), 0, n_bytes);
  }

  /**
   * @brief Gets the type of a lock.
   *
   * This function retrieves the type of the specified lock. It is a non-inline
   * version intended for use outside of the lock module.
   *
   * @return The type of the lock, either LOCK_TABLE or LOCK_REC.
   */
  [[nodiscard]] ulint get_type() const noexcept;

  /**
   * @brief Gets the type of a lock in a human readable string.
   *
   * This function retrieves the type of the specified lock and returns it as a
   * human-readable string. The returned string should not be free()'d or modified.
   *
   * @return The lock type as a human-readable string.
   */
  [[nodiscard]] const char *get_type_str() const noexcept;

  /**
   * @brief Gets the table on which the lock is.
   *
   * This function retrieves the table associated with the given lock.
   * Depending on the type of the lock (record or table), it returns
   * the corresponding table.
   *
   * @return dict_table_t* Pointer to the table associated with the lock.
   */
  [[nodiscard]] inline const Table *table() const noexcept;

  /**
   * @brief Gets the id of the table on which the lock is.
   *
   * This function retrieves the id of the table associated with the specified lock.
   *
   * @return The id of the table on which the lock is.
   */
  [[nodiscard]] inline uint64_t table_id() const noexcept;

  /**
   * @brief Gets the name of the table on which the lock is.
   *
   * This function retrieves the name of the table associated with the specified lock.
   * The returned string should not be free()'d or modified.
   *
   * @return The name of the table on which the lock is.
   */
  [[nodiscard]] inline const char *table_name() const noexcept;

  /**
   * @brief Gets the index on which the record lock is.
   *
   * This function retrieves the index associated with the specified record lock.
   *
   * @return The index on which the record lock is.
   */
  [[nodiscard]] inline const Index *rec_index() const noexcept {
    ut_a(type() == LOCK_REC);
    return m_rec.m_index;
  }

  /**
   * @brief Gets the index on which the record lock is.
   *
   * This function retrieves the index associated with the specified record lock.
   *
   * @return The index on which the record lock is.
   */
  [[nodiscard]] inline Index *rec_index() noexcept { return const_cast<Index *>(const_cast<const Lock *>(this)->rec_index()); }

  /**
   * @brief Gets the name of the index on which the record lock is.
   *
   * This function retrieves the name of the index associated with the specified
   * record lock. The returned string should not be free()'d or modified.
   *
   * @return The name of the index on which the lock is.
   */
  [[nodiscard]] inline const char *rec_index_name() const noexcept;

  /**
   * @brief Gets the tablespace number for a record lock.
   *
   * This function retrieves the tablespace number associated with the specified
   * record lock.
   *
   * @return The tablespace number on which the lock is.
   */
  [[nodiscard]] inline space_id_t rec_space_id() const noexcept {
    ut_ad(type() == LOCK_REC);
    return m_rec.m_page_id.m_space_id;
  }

  /**
   * @brief Gets the page number on which the record lock is.
   *
   * This function retrieves the page number associated with the specified record lock.
   *
   * @return The page number on which the lock is.
   */
  [[nodiscard]] inline page_no_t rec_page_no() const noexcept {
    ut_ad(type() == LOCK_REC);
    return m_rec.m_page_id.m_page_no;
  }

  /**
   * @brief Copies a record lock to the specified memory heap.
   *
   * This function creates a copy of the given record lock and stores it in the provided memory heap.
   * The copied lock includes the lock structure and its associated bitmap.
   *
   * @param[in] heap The memory heap where the lock copy will be stored.
   *
   * @return A pointer to the copied lock.
   */
  [[nodiscard]] inline Lock *rec_clone(mem_heap_t *heap) const noexcept;

  /**
   * @brief Gets the mode of a lock in a human readable string.
   *
   * This function retrieves the mode of the specified lock and returns it as a
   * human-readable string. The returned string should not be free()'d or modified.
   *
   * @return The lock mode as a human-readable string.
   */
  [[nodiscard]] inline const char *get_mode_str() const noexcept;

  /**
   * @brief Gets the page id of a lock.
   *
   * This function retrieves the page id of the specified lock.
   *
   * @return The page id of the lock.
   */
  [[nodiscard]] inline Page_id page_id() const noexcept {
    ut_ad(type() == LOCK_REC);
    return m_rec.m_page_id;
  }

  /**
   * @brief Converts the table lock object to a string representation.
   *
   * This function generates a string that represents the current state
   * of the table lock object. The string includes details such as the table ID.
   *
   * @param[in] buf_pool The buffer pool to use for page retrieval.
   *
   * @return A string representation of the table lock object.
   */
  std::string table_to_string() const noexcept;

  /**
   * @brief Converts the record lock object to a string representation.
   *
   * This function generates a string that represents the current state
   * of the record lock object. The string includes details such as the
   * space ID and page number.
   *
   * @param[in] buf_pool The buffer pool to use for page retrieval.
   *
   * @return A string representation of the record lock object.
   */
  std::string rec_to_string(Buf_pool *buf_pool) const noexcept;

  /**
   * @brief Converts the lock object to a string representation.
   *
   * This function generates a string that represents the current state
   * of the lock object. The string includes details such as the lock type,
   * mode, and other relevant information.
   *
   * @return A string representation of the lock object.
   */
  [[nodiscard]] std::string to_string(Buf_pool *buf_pool) const noexcept;

  /**
   * @brief Gets the next lock in the list.
   *
   * This function retrieves the next lock in the list of locks.
   *
   * @return A pointer to the next lock in the list.
   */
  [[nodiscard]] Lock *next() const noexcept {
    ut_ad(type() == LOCK_REC);
    return UT_LIST_GET_NEXT(m_rec.m_rec_locks, const_cast<Lock *>(this));
  }

  /**
   * @brief Gets the previous lock in the list.
   *
   * This function retrieves the previous lock in the list of locks.
   *
   * @return A pointer to the previous lock in the list.
   */
  [[nodiscard]] Lock *prev() const noexcept {
    ut_ad(type() == LOCK_REC);
    return UT_LIST_GET_PREV(m_rec.m_rec_locks, const_cast<Lock *>(this));
  }

  /**
   * @brief Determines if one lock mode is stronger or equal to another lock mode.
   *
   * This function checks if the first lock mode (mode1) is stronger than or equal to
   * the second lock mode (mode2). The strength of lock modes is determined based on
   * predefined rules in the lock system.
   *
   * @param[in] lhs The first lock mode to compare.
   * @param[in] rhs The second lock mode to compare.
   *
   * @return true if lhs stronger or equal to rhs
   */
  [[nodiscard]] static bool mode_stronger_or_eq(Lock_mode lhs, Lock_mode rhs) noexcept;

  /**
   * @brief Determines if two lock modes are compatible.
   *
   * This function checks if the first lock mode (mode1) is compatible with
   * the second lock mode (mode2). Compatibility is determined based on
   * predefined rules in the lock system.
   *
   * @param[in] lhs The first lock mode to compare.
   * @param[in] rhs The second lock mode to compare.
   *
   * @return true if lhs is compatible with rhs , falseotherwise.
   */
  [[nodiscard]] static inline bool mode_compatible(Lock_mode lhs, Lock_mode rhs) noexcept;

  /** Transaction owning the lock */
  union {
    /** Table lock */
    Table_lock m_table;

    /** Record lock */
    Rec_lock m_rec;
  };

  Trx *m_trx;

  /** List of the locks of the transaction */
  UT_LIST_NODE_T(Lock) m_trx_locks;

  /** Number of bits in the lock bitmap; NOTE: the lock bitmap
  is placed immediately after the lock struct */
  uint32_t m_n_bits;

  /** Lock type, mode, LOCK_GAP or LOCK_REC_NOT_GAP, LOCK_INSERT_INTENTION, wait flag, ORed */
  Lock_mode m_type_mode;
};

/** Try and keep the size of Lock to 64 bytes so that it can fit in a cache line.*/
static_assert(sizeof(Lock) <= 64, "Lock must be 64 bytes or smaller");

/** We need this to read beyond Lock. */
static_assert(std::is_standard_layout<Lock>::value, "Lock must have a standard layout");

/** Lock operation struct */
struct Lock_op {
  /** Table to be locked */
  Table *m_table;

  /** Lock mode */
  Lock_mode mode;
};

/**
 * List of locks that different transactions have acquired on a record. This
 * list has a list node that is embedded in a nested union/structure. We have to
 * generate a specific template for it. See lock0lock.cc for the implementation.
 */
struct Rec_lock_get_node;

/**
 * @brief List of locks that different transactions have acquired on a record.
 *
 * This type is used to store a list of locks that different transactions have
 * acquired on a record. The list has a list node that is embedded in a nested
 * union/structure. We have to generate a specific template for it. See lock0lock.cc
 * for the implementation.
 */
using Rec_locks = ut_list_base<Lock, Rec_lock_get_node>;
