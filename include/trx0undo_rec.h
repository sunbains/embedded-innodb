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

/** @file include/trx0undo_rec.h
Transaction undo log record parsing and building utilities

*******************************************************/

#pragma once

#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "mach0data.h"
#include "mtr0mtr.h"
#include "que0types.h"
#include "rem0types.h"
#include "row0types.h"
#include "trx0types.h"
#include "ut0byte.h"

/* Type constants */
/** Fresh insert into clustered index */
constexpr ulint TRX_UNDO_INSERT_REC = 11;

/** Update of a non-delete-marked record */
constexpr ulint TRX_UNDO_UPD_EXIST_REC = 12;

/** Update of a delete marked record to a not delete marked record; also the fields of the record can change */
constexpr ulint TRX_UNDO_UPD_DEL_REC = 13;

/** Delete marking of a record; fields do not change */
constexpr ulint TRX_UNDO_DEL_MARK_REC = 14;

/** Compilation info is multiplied by this and ORed to the type above */
constexpr ulint TRX_UNDO_CMPL_INFO_MULT = 16;

/** This bit can be ORed to type_cmpl to denote that we updated external
storage fields: used by purge to free the external storage */
constexpr ulint TRX_UNDO_UPD_EXTERN = 128;

/* Operation type flags used in trx_undo_report_row_operation */
constexpr ulint TRX_UNDO_INSERT_OP = 1;
constexpr ulint TRX_UNDO_MODIFY_OP = 2;

/**
 * @brief Transaction undo record wrapper class
 *
 * This class wraps trx_undo_rec_t  providing a clean interface for parsing, building,
 * and manipulating undo records. It eliminates the need for raw byte pointer manipulation.
 */
struct Trx_undo_record {
  struct Parsed {
    /** TRX_UNDO_INSERT_REC, TRX_UNDO_UPD_EXIST_REC, TRX_UNDO_UPD_DEL_REC, TRX_UNDO_DEL_MARK_REC */
    ulint m_type{ULINT_UNDEFINED};

    /** Compiler info, relevant only for update type records */
    ulint m_cmpl_info{ULINT_UNDEFINED};

    /** Table id */
    Dict_id m_table_id{DICT_ID_NULL};

    /** Undo log record number */
    undo_no_t m_undo_no{TRX_ID_UNDEFINED};

    /** True if we updated an externally stored field */
    bool m_extern{false};
  };

  /** Default constructor */
  Trx_undo_record() : m_undo_rec() {}

  /** Implicit constructor.
  * @param[in] undo_rec - Undo record to wrap
  */
  Trx_undo_record(trx_undo_rec_t undo_rec) : m_undo_rec(undo_rec) {}

  /** Constructor from trx_undo_rec_t  */
  // explicit Trx_undo_record(trx_undo_rec_t undo_rec) : m_undo_rec(undo_rec) {}

  /** Get the raw pointer */
  trx_undo_rec_t raw() const noexcept { return m_undo_rec; }

  /** Set the raw pointer */
  void set(trx_undo_rec_t undo_rec) noexcept { m_undo_rec = undo_rec; }

  /** Check if valid */
  bool is_valid() const noexcept { return m_undo_rec != nullptr; }

  /** Implicit conversion to raw pointer for compatibility */
  operator trx_undo_rec_t() const noexcept { return m_undo_rec; }

  /**
   * @brief Writes information to an undo log about an insert, update, or a delete
   * marking of a clustered index record.
   *
   * @param[in] flags If BTR_NO_UNDO_LOG_FLAG bit is set, does nothing
   * @param[in] op_type TRX_UNDO_INSERT_OP or TRX_UNDO_MODIFY_OP
   * @param[in] thr Query thread
   * @param[in] index Clustered index
   * @param[in] clust_entry In the case of an insert, index entry to insert into the clustered index, otherwise nullptr
   * @param[in] update In the case of an update, the update vector, otherwise nullptr
   * @param[in] cmpl_info Compiler info on secondary index updates
   * @param[in] rec In the case of an update or delete marking, the record in the clustered index, otherwise nullptr
   * @param[out] roll_ptr Rollback pointer to the inserted undo log record, 0 if BTR_NO_UNDO_LOG flag was specified
   *
   * @return DB_SUCCESS or error code
   */
  static db_err report_row_operation(
    ulint flags, ulint op_type, que_thr_t *thr, const Index *index, const DTuple *clust_entry, const upd_t *update, ulint cmpl_info,
    const Rec rec, roll_ptr_t *roll_ptr
  );

  /**
   * @brief Copies an undo record to heap. This function can be called if we know that
   * the undo log record exists.
   * @param[in] roll_ptr roll pointer to record
   * @param[in] heap memory heap where copied
   * @return own: copy of the record
   */
  static Trx_undo_record get_undo_rec_low(roll_ptr_t roll_ptr, mem_heap_t *heap);

  /**
   * @brief Copies an undo record to heap.
   *
   * NOTE: the caller must have latches on the clustered index page and
   * purge_view.
   *
   * @param[in] roll_ptr roll pointer to record
   * @param[in] trx_id id of the trx that generated the roll pointer: it points to an
   *            undo log of this transaction
   * @param[out] undo_rec own: copy of the record
   * @param[in] heap memory heap where copied
   * @return DB_SUCCESS, or DB_MISSING_HISTORY if the undo log has been
   * truncated and we cannot fetch the old version
   */
  static db_err get_undo_rec(roll_ptr_t roll_ptr, trx_id_t trx_id, Trx_undo_record *undo_rec, mem_heap_t *heap);

  /**
   * @brief Build a previous version of a clustered index record.
   * @param[in] index_rec clustered index record in the index tree
   * @param[in] index_mtr mtr which contains the latch to index_rec page and purge_view
   * @param[in] rec version of a clustered index record
   * @param[in] index clustered index
   * @param[in] offsets Phy_rec::get_col_offsets(rec, index)
   * @param[in] heap memory heap from which the memory needed is allocated
   * @param[out] old_vers own: previous version, or nullptr if rec is the first inserted version, or if history data has been deleted
   * @return DB_SUCCESS, or DB_MISSING_HISTORY if the previous version is
   * earlier than purge_view, which means that it may have been removed,
   * DB_ERROR if corrupted record
   */
  static db_err prev_version_build(
    const Rec index_rec, mtr_t *index_mtr, const Rec rec, Index *index, ulint *offsets, mem_heap_t *heap, Rec *old_vers
  );

  /**
   * @brief Reads from this undo log record the general parameters.
   *
   * @param[out] pars Parsed undo record.
   *
   * @return Pointer to the remaining part of the undo log record after reading these values.
   */
  byte *get_pars(Parsed &undo_rec_pars) const noexcept;

  /**
   * @brief Static version for compatibility - reads from an undo log record the general parameters.
   *
   * @param[in] undo_rec Undo log record.
   * @param[out] pars Parsed undo record.
   *
   * @return Pointer to the remaining part of the undo log record after reading these values.
   */
  static byte *get_pars(trx_undo_rec_t undo_rec, Parsed &undo_rec_pars) noexcept;

  /**
   * @brief Builds a row reference from an undo log record.
   * @param[in] ptr remaining part of a copy of an undo log record, at the start of the row
   *            reference; NOTE that this copy of the undo log record must be preserved as long as the
   *            row reference is used, as we do NOT copy the data in the record!
   * @param[in] index clustered index
   * @param[out] ref own: row reference
   * @param[in] heap memory heap from which the memory needed is allocated
   * @return pointer to remaining part of undo record
   */
  static byte *get_row_ref(byte *ptr, Index *index, DTuple **ref, mem_heap_t *heap);

  /**
   * @brief Skips a row reference from an undo log record.
   * @param[in] ptr remaining part in update undo log record, at the start of the row reference
   * @param[in] index clustered index
   * @return pointer to remaining part of undo record
   */
  static byte *skip_row_ref(byte *ptr, Index *index);

  /**
   * @brief Reads from an undo log update record the system field values of the old version.
   * @param[in] ptr remaining part of undo log record after reading general parameters
   * @param[out] trx_id trx id
   * @param[out] roll_ptr roll ptr
   * @param[out] info_bits info bits state
   * @return remaining part of undo log record after reading these values
   */
  static byte *update_rec_get_sys_cols(byte *ptr, trx_id_t *trx_id, roll_ptr_t *roll_ptr, ulint *info_bits);

  /**
   * @brief Builds an update vector based on a remaining part of an undo log record.
   * @param[in] ptr remaining part in update undo log record, after reading the row reference
   *            NOTE that this copy of the undo log record must be preserved as long as the update vector is
   *            used, as we do NOT copy the data in the record!
   * @param[in] index clustered index
   * @param[in] type TRX_UNDO_UPD_EXIST_REC, TRX_UNDO_UPD_DEL_REC, or TRX_UNDO_DEL_MARK_REC; in the last case,
   *            only trx id and roll ptr fields are added to the update vector
   * @param[in] trx_id transaction id from this undorecord
   * @param[in] roll_ptr roll pointer from this undo record
   * @param[in] info_bits info bits from this undo record
   * @param[in] trx transaction
   * @param[in] heap memory heap from which the memory needed is allocated
   * @param[out] upd own: update vector
   * @return remaining part of the record, nullptr if an error detected, which
   * means that the record is corrupted
   */
  static byte *update_rec_get_update(
    byte *ptr, Index *index, ulint type, trx_id_t trx_id, roll_ptr_t roll_ptr, ulint info_bits, Trx *trx, mem_heap_t *heap,
    upd_t **upd
  );

  /**
   * @brief Builds a partial row from an update undo log record.
   * @param[in] ptr remaining part in update undo log record of a suitable type, at the start of
   *            the stored index columns; NOTE that this copy of the undo log record must
   *            be preserved as long as the partial row is used, as we do NOT copy the data in the
   *            record!
   * @param[in] index clustered index
   * @param[out] row own: partial row
   * @param[in] ignore_prefix flag to indicate if we expect blob prefixes in undo. Used
   *            only in the assertion.
   * @param[in] heap memory heap from which the memory needed is allocated
   * @return pointer to remaining part of undo record
   */
  static byte *get_partial_row(byte *ptr, Index *index, DTuple **row, bool ignore_prefix, mem_heap_t *heap);

  /**
   * @brief Parses a redo log record of adding an undo log record.
   * @param[in] ptr buffer
   * @param[in] end_ptr buffer end
   * @param[in] page page or nullptr
   * @return end of log record or nullptr
   */
  static byte *parse_add_undo_rec(byte *ptr, byte *end_ptr, page_t *page);

  /**
   * @brief Parses a redo log record of erasing of an undo page end.
   * @param[in] ptr buffer
   * @param[in] end_ptr buffer end
   * @param[in] page page or nullptr
   * @param[in] mtr mtr or nullptr
   * @return end of log record or nullptr
   */
  static byte *parse_erase_page_end(byte *ptr, byte *end_ptr, page_t *page, mtr_t *mtr);

  /**
   * @brief Copies this undo record to the heap.
   * @param[in,out] heap Heap to use for storing the copy.
   * @return own: copy of undo log record
   */
  Trx_undo_record copy(mem_heap_t *heap) const {
    const auto len = mach_read_from_2(m_undo_rec) - ut_align_offset(m_undo_rec, UNIV_PAGE_SIZE);
    auto copied = reinterpret_cast<trx_undo_rec_t>(mem_heap_dup(heap, m_undo_rec, len));
    return Trx_undo_record(copied);
  }

  /**
   * @brief Static version for compatibility - copies the undo record to the heap.
   * @param[in] undo_rec   Undo log record.
   * @param[in,out] heap Heap to use for storing the copy.
   * @return own: copy of undo log record
   */
  static trx_undo_rec_t copy(const trx_undo_rec_t undo_rec, mem_heap_t *heap) {
    const auto len = mach_read_from_2(undo_rec) - ut_align_offset(undo_rec, UNIV_PAGE_SIZE);
    return reinterpret_cast<trx_undo_rec_t>(mem_heap_dup(heap, undo_rec, len));
  }

  // Inline utility functions

  /**
   * @brief Reads from this undo log record the record type.
   * @return record type
   */
  ulint get_type() const { return mach_read_from_1(m_undo_rec + 2) & (TRX_UNDO_CMPL_INFO_MULT - 1); }

  /**
   * @brief Static version for compatibility - reads from an undo log record the record type.
   * @param[in] undo_rec undo log record
   * @return record type
   */
  static ulint get_type(const trx_undo_rec_t undo_rec) { return mach_read_from_1(undo_rec + 2) & (TRX_UNDO_CMPL_INFO_MULT - 1); }

  /**
   * @brief Reads from this undo log record the record compiler info.
   * @return compiler info
   */
  ulint get_cmpl_info() const { return mach_read_from_1(m_undo_rec + 2) / TRX_UNDO_CMPL_INFO_MULT; }

  /**
   * @brief Static version for compatibility - reads from an undo log record the record compiler info.
   * @param[in] undo_rec undo log record
   * @return compiler info
   */
  static ulint get_cmpl_info(const trx_undo_rec_t undo_rec) { return mach_read_from_1(undo_rec + 2) / TRX_UNDO_CMPL_INFO_MULT; }

  /**
   * @brief Returns true if this undo log record contains an extern storage field.
   * @return true if extern
   */
  bool get_extern_storage() const { return (mach_read_from_1(m_undo_rec + 2) & TRX_UNDO_UPD_EXTERN) != 0; }

  /**
   * @brief Static version for compatibility - returns true if an undo log record contains an extern storage field.
   * @param[in] undo_rec undo log record
   * @return true if extern
   */
  static bool get_extern_storage(const trx_undo_rec_t undo_rec) {
    return (mach_read_from_1(undo_rec + 2) & TRX_UNDO_UPD_EXTERN) != 0;
  }

  /**
   * @brief Reads the undo log record number from this record.
   * @return undo no
   */
  undo_no_t get_undo_no() const { return mach_uint64_read_much_compressed(m_undo_rec + 3); }

  /**
   * @brief Static version for compatibility - reads the undo log record number.
   * @param[in] undo_rec undo log record
   * @return undo no
   */
  static undo_no_t get_undo_no(const trx_undo_rec_t undo_rec) { return mach_uint64_read_much_compressed(undo_rec + 3); }

  /**
   * @brief Returns the start of the undo record data area.
   * @param[in] undo_no undo no read from node
   * @return offset to the data area
   */
  static ulint get_offset(undo_no_t undo_no) { return mach_uint64_get_much_compressed_size(undo_no) + 3; }

 private:
  trx_undo_rec_t m_undo_rec{};
};
