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

/** @file include/trx0rec.h
Transaction undo log record

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#pragma once

#include "data0data.h"
#include "dict0types.h"
#include "innodb0types.h"
#include "mtr0mtr.h"
#include "que0types.h"
#include "rem0types.h"
#include "row0types.h"
#include "trx0types.h"

struct Undo_rec_pars {
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

/* FIXME: From merge */
/** Returns the start of the undo record data area. */
#define trx_undo_rec_get_ptr(undo_rec, undo_no) ((undo_rec) + trx_undo_rec_get_offset(undo_no))

/**
 * @brief Reads from an undo log record the general parameters.
 *
 * @param[in] undo_rec Undo log record.
 * @param[out] pars Parsed undo record.
 * 
 * @return Pointer to the remaining part of the undo log record after reading these values.
 */
byte *trx_undo_rec_get_pars(trx_undo_rec_t *undo_rec, Undo_rec_pars &undo_rec_pars) noexcept;

/** Builds a row reference from an undo log record.
@return	pointer to remaining part of undo record */
byte *trx_undo_rec_get_row_ref(
  byte *ptr,       /*!< in: remaining part of a copy of an undo
                                    log record, at the start of the row
                                    reference; NOTE that this copy of the undo
                                    log record must be preserved as long as the
                                    row reference is used, as we do NOT copy the
                                    data in the record! */
  Index *index,    /*!< in: clustered index */
  DTuple **ref,    /*!< out, own: row reference */
  mem_heap_t *heap /*!< in: memory heap from which the memory needed is allocated */
);

/** Skips a row reference from an undo log record.
@return	pointer to remaining part of undo record */
byte *trx_undo_rec_skip_row_ref(
  byte *ptr,   /*!< in: remaining part in update undo log
                          record, at the start of the row reference */
  Index *index /*!< in: clustered index */
);

/** Reads from an undo log update record the system field values of the old
version.
@return	remaining part of undo log record after reading these values */
byte *trx_undo_update_rec_get_sys_cols(
  byte *ptr,            /*!< in: remaining part of undo
                                            log record after reading
                                            general parameters */
  trx_id_t *trx_id,     /*!< out: trx id */
  roll_ptr_t *roll_ptr, /*!< out: roll ptr */
  ulint *info_bits      /*!< out: info bits state */
);

/** Builds an update vector based on a remaining part of an undo log record.
@return remaining part of the record, nullptr if an error detected, which
means that the record is corrupted */
byte *trx_undo_update_rec_get_update(
  byte *ptr,           /*!< in: remaining part in update undo log
                         record, after reading the row reference
                         NOTE that this copy of the undo log record must
                         be preserved as long as the update vector is
                         used, as we do NOT copy the data in the
                         record! */
  Index *index,        /*!< in: clustered index */
  ulint type,          /*!< in: TRX_UNDO_UPD_EXIST_REC,
                         TRX_UNDO_UPD_DEL_REC, or
                         TRX_UNDO_DEL_MARK_REC; in the last case,
                         only trx id and roll ptr fields are added to
                         the update vector */
  trx_id_t trx_id,     /*!< in: transaction id from this undorecord */
  roll_ptr_t roll_ptr, /*!< in: roll pointer from this undo record */
  ulint info_bits,     /*!< in: info bits from this undo record */
  Trx *trx,            /*!< in: transaction */
  mem_heap_t *heap,    /*!< in: memory heap from which the memory
                         needed is allocated */
  upd_t **upd          /*!< out, own: update vector */
);

/** Builds a partial row from an update undo log record. It contains the
columns which occur as ordering in any index of the table.
@return	pointer to remaining part of undo record */
byte *trx_undo_rec_get_partial_row(
  byte *ptr,          /*!< in: remaining part in update undo log
                         record of a suitable type, at the start of
                         the stored index columns;
                         NOTE that this copy of the undo log record must
                         be preserved as long as the partial row is
                         used, as we do NOT copy the data in the
                         record! */
  Index *index,       /*!< in: clustered index */
  DTuple **row,       /*!< out, own: partial row */
  bool ignore_prefix, /*!< in: flag to indicate if we
                   expect blob prefixes in undo. Used
                   only in the assertion. */
  mem_heap_t *heap    /*!< in: memory heap from which the memory needed is allocated */
);

/**
 * @brief Writes information to an undo log about an insert, update, or a delete
 * marking of a clustered index record. This information is used in a rollback of
 * the transaction and in consistent reads that must look to the history of this
 * transaction.
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
db_err trx_undo_report_row_operation(
  ulint flags, ulint op_type, que_thr_t *thr, const Index *index, const DTuple *clust_entry, const upd_t *update, ulint cmpl_info,
  const rec_t *rec, roll_ptr_t *roll_ptr
);

/** Copies an undo record to heap. This function can be called if we know that
the undo log record exists.
@return	own: copy of the record */
trx_undo_rec_t *trx_undo_get_undo_rec_low(
  roll_ptr_t roll_ptr, /*!< in: roll pointer to record */
  mem_heap_t *heap     /*!< in: memory heap where copied */
);

/** Copies an undo record to heap.

NOTE: the caller must have latches on the clustered index page and
purge_view.

@return DB_SUCCESS, or DB_MISSING_HISTORY if the undo log has been
truncated and we cannot fetch the old version */
db_err trx_undo_get_undo_rec(
  roll_ptr_t roll_ptr,       /*!< in: roll pointer to record */
  trx_id_t trx_id,           /*!< in: id of the trx that generated
                               the roll pointer: it points to an
                               undo log of this transaction */
  trx_undo_rec_t **undo_rec, /*!< out, own: copy of the record */
  mem_heap_t *heap           /*!< in: memory heap where copied */
);

/** Build a previous version of a clustered index record. This function checks
that the caller has a latch on the index page of the clustered index record
and an s-latch on the purge_view. This guarantees that the stack of versions
is locked.
@return DB_SUCCESS, or DB_MISSING_HISTORY if the previous version is
earlier than purge_view, which means that it may have been removed,
DB_ERROR if corrupted record */
db_err trx_undo_prev_version_build(
  const rec_t *index_rec, /*!< in: clustered index record in the
                          index tree */
  mtr_t *index_mtr,       /*!< in: mtr which contains the latch to
                          index_rec page and purge_view */
  const rec_t *rec,       /*!< in: version of a clustered index record */
  Index *index,           /*!< in: clustered index */
  ulint *offsets,         /*!< in: Phy_rec::get_col_offsets(rec, index) */
  mem_heap_t *heap,       /*!< in: memory heap from which the memory
                            needed is allocated */
  rec_t **
    old_vers /*!< out, own: previous version, or nullptr if rec is the first inserted version, or if history data has been deleted */
);

/** Parses a redo log record of adding an undo log record.
@return	end of log record or nullptr */
byte *trx_undo_parse_add_undo_rec(
  byte *ptr,     /*!< in: buffer */
  byte *end_ptr, /*!< in: buffer end */
  page_t *page   /*!< in: page or nullptr */
);

/** Parses a redo log record of erasing of an undo page end.
@return	end of log record or nullptr */
byte *trx_undo_parse_erase_page_end(
  byte *ptr,     /*!< in: buffer */
  byte *end_ptr, /*!< in: buffer end */
  page_t *page,  /*!< in: page or nullptr */
  mtr_t *mtr     /*!< in: mtr or nullptr */
);

/* Types of an undo log record: these have to be smaller than 16, as the
compilation info multiplied by 16 is ORed to this value in an undo log
record */

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

/** Reads from an undo log record the record type.
@return        record type */
inline ulint trx_undo_rec_get_type(const trx_undo_rec_t *undo_rec) /*!< in: undo log record */
{
  return mach_read_from_1(undo_rec + 2) & (TRX_UNDO_CMPL_INFO_MULT - 1);
}

/** Reads from an undo log record the record compiler info.
@return        compiler info */
inline ulint trx_undo_rec_get_cmpl_info(const trx_undo_rec_t *undo_rec) /*!< in: undo log record */
{
  return mach_read_from_1(undo_rec + 2) / TRX_UNDO_CMPL_INFO_MULT;
}

/** Returns true if an undo log record contains an extern storage field.
@return        true if extern */
inline bool trx_undo_rec_get_extern_storage(const trx_undo_rec_t *undo_rec) /*!< in: undo log record */
{
  return (mach_read_from_1(undo_rec + 2) & TRX_UNDO_UPD_EXTERN) != 0;
}

/** Reads the undo log record number.
@return        undo no */
inline undo_no_t trx_undo_rec_get_undo_no(const trx_undo_rec_t *undo_rec) /*!< in: undo log record */
{
  return mach_uint64_read_much_compressed(undo_rec + 3);
}

/** Returns the start of the undo record data area.
@return        offset to the data area */
inline ulint trx_undo_rec_get_offset(undo_no_t undo_no) /*!< in: undo no read from node */
{
  return mach_uint64_get_much_compressed_size(undo_no) + 3;
}

/** Copies the undo record to the heap.
@param[in] undo_rec   Undo log record.
@param[in,out] heap Heap to use for storing the copy.
@return        own: copy of undo log record */
inline trx_undo_rec_t *trx_undo_rec_copy(const trx_undo_rec_t *undo_rec, mem_heap_t *heap) {
  const auto len = mach_read_from_2(undo_rec) - ut_align_offset(undo_rec, UNIV_PAGE_SIZE);
  return reinterpret_cast<trx_undo_rec_t *>(mem_heap_dup(heap, undo_rec, len));
}
