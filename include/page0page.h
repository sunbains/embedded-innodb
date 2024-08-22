/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/page0page.h
Index page routines

Created 2/2/1994 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "buf0buf.h"
#include "data0data.h"
#include "dict0dict.h"
#include "fil0fil.h"
#include "fsp0fsp.h"
#include "mtr0mtr.h"
#include "page0types.h"
#include "rem0rec.h"
#include "mach0data.h"
#ifdef UNIV_DEBUG
#include "log0recv.h"
#endif /* !UNIV_DEBUG */
#include "mtr0log.h"
#include "rem0cmp.h"

/*			PAGE HEADER
Index page header starts at the first offset left free by the fil-module */

using page_header_t = byte;

/** index page header starts at this offset */
constexpr auto PAGE_HEADER = FSEG_PAGE_DATA;

/** Number of slots in page directory */
constexpr ulint PAGE_N_DIR_SLOTS = 0;

/** Pointer to record heap top */
constexpr ulint PAGE_HEAP_TOP = 2;

/** Number of records in the heap */
constexpr ulint PAGE_N_HEAP = 4;

/* Pointer to start of page free record list */
constexpr ulint PAGE_FREE = 6;

/** Number of bytes in deleted records */
constexpr ulint PAGE_GARBAGE = 8;

/** Pointer to the last inserted record, or NULL if
this info has been reset by a delete, for example */
constexpr ulint PAGE_LAST_INSERT = 10;

/** Last insert direction: PAGE_LEFT, ... */
constexpr ulint PAGE_DIRECTION = 12;

/** Number of consecutive inserts to the same direction */
constexpr ulint PAGE_N_DIRECTION = 14;

/* number of user records on the page */
constexpr ulint PAGE_N_RECS = 16;

/** Highest id of a trx which may have modified a record on the page;
a uint64_t; defined only in secondary indexes; NOTE: this may be
modified only when the thread has an x-latch to the page */
constexpr ulint PAGE_MAX_TRX_ID = 18;

/** End of private data structure of the page
header which are set in a page create */
constexpr ulint PAGE_HEADER_PRIV_END = 26;

/* level of the node in an index tree; the
leaf level is the level 0.  This field should
not be written to after page creation. */
constexpr ulint PAGE_LEVEL = 26;

/** index id where the page belongs.
This field should not be written to after
page creation. */
constexpr ulint PAGE_INDEX_ID = 28;

/* file segment header for the leaf pages in
a B-tree: defined only on the root page of a
B-tree. */
constexpr ulint PAGE_BTR_SEG_LEAF = 36L;

/** file segment header for the non-leaf pages
in a B-tree: defined only on the root page of
a B-tree tree */
constexpr ulint PAGE_BTR_SEG_TOP = 36 + FSEG_HEADER_SIZE;

/** Start of data on the page */
constexpr ulint PAGE_DATA = PAGE_HEADER + 36 + 2 * FSEG_HEADER_SIZE;

/** Offset of the page infimum record on a page */
constexpr ulint PAGE_INFIMUM = PAGE_DATA + 1 + REC_N_EXTRA_BYTES;

/** Offset of the page supremum record on a page */
constexpr ulint PAGE_SUPREMUM = PAGE_DATA + 2 + 2 * REC_N_EXTRA_BYTES + 8;

/** Offset of the page supremum record end on a page */
constexpr ulint PAGE_SUPREMUM_END = PAGE_SUPREMUM + 9;

/*-----------------------------*/

/* Heap numbers */

/** Page infimum */
constexpr ulint PAGE_HEAP_NO_INFIMUM = 0;

/** Page supremum */
constexpr ulint PAGE_HEAP_NO_SUPREMUM = 1;

/** First user record in creation (insertion) order, not necessarily
collation order; this record may have been deleted */
constexpr ulint PAGE_HEAP_NO_USER_LOW = 2;

/* Directions of cursor movement */
constexpr ulint PAGE_LEFT = 1;
constexpr ulint PAGE_RIGHT = 2;
constexpr ulint PAGE_SAME_REC = 3;
constexpr ulint PAGE_SAME_PAGE = 4;
constexpr ulint PAGE_NO_DIRECTION = 5;

/*	PAGE DIRECTORY */

using page_dir_slot_t = byte;
using page_dir_t = page_dir_slot_t;

/** Offset of the directory start down from the page end. We call the
slot with the highest file address directory start, as it points to
the first record in the list of records. */
constexpr ulint PAGE_DIR = FIL_PAGE_DATA_END;

/** We define a slot in the page directory as two bytes */
constexpr ulint PAGE_DIR_SLOT_SIZE = 2;

/** The offset of the physically lower end of the directory, counted from
page end, when the page is empty */
constexpr ulint PAGE_EMPTY_DIR_START = (PAGE_DIR + 2 * PAGE_DIR_SLOT_SIZE);

/* The maximum and minimum number of records owned by a directory slot. The
number may drop below the minimum in the first and the last slot in the
directory. */
constexpr ulint PAGE_DIR_SLOT_MAX_N_OWNED = 8;
constexpr ulint PAGE_DIR_SLOT_MIN_N_OWNED = 4;

/**
 * Set the maximum transaction ID on the page.
 * 
 * @param[in] block page
 * @param[in] trx_id transaction ID
 * @param[in] mtr mini-transaction
 */
void page_set_max_trx_id(buf_block_t *block, trx_id_t trx_id, mtr_t *mtr);

#define page_get_infimum_rec(page) ((page) + page_get_infimum_offset(page))

/**
 * Reads the given header field.
 * 
 * @param[in] page page to read from
 * @param[in] field field to read within the page.
 */
inline ulint page_header_get_field(const page_t *page, ulint field) {
  ut_ad(field <= PAGE_INDEX_ID);

  return mach_read_from_2(page + PAGE_HEADER + field);
}

/**
 * Returns the offset stored in the given header field.
 * 
 * @param[in] page page to read from
 * @param[in] field field to read within the page. PAGE_FREE, ...
 * @return	offset from the start of the page, or 0 */
inline ulint page_header_get_offs(const page_t *page, ulint field) {
  ut_ad(field == PAGE_FREE || field == PAGE_LAST_INSERT || field == PAGE_HEAP_TOP);

  const auto offs = page_header_get_field(page, field);

  ut_ad(field != PAGE_HEAP_TOP || offs > 0);

  return offs;
}

/**
 * Returns the pointer stored in the given header field, or NULL.
 * 
 * @param[in] page page to read from
 * @param[in] field field to read within the page.
 */
inline page_t *page_header_get_ptr(page_t *page, ulint field) {
  return page_header_get_offs(page, field) ? page + page_header_get_offs(page, field) : nullptr;
}

/**
 * Determine whether the page is a B-tree leaf.
 * 
 * @param[in] page page to check
 * 
 * @return	true if the page is a B-tree leaf
 */
inline bool page_is_leaf(const page_t *page) {
  return *reinterpret_cast<const uint16_t *>(page + (PAGE_HEADER + PAGE_LEVEL)) == 0;
}

#define page_get_supremum_rec(page) ((page) + page_get_supremum_offset(page))

/**
 * Returns the middle record of record list. If there are an even number
 * of records in the list, returns the first record of upper half-list.
 * 
 * @param[in] page page to read from
 * 
 * @return	middle record */
rec_t *page_get_middle_rec(page_t *page);

/**
 * Returns the number of records before the given record in chain.
 * The number includes infimum and supremum records.
 * 
 * @param[in] rec the physical record
 * 
 * @return        number of records
 */
ulint page_rec_get_n_recs_before(const rec_t *rec);

#ifndef UNIV_DEBUG
#define page_dir_get_nth_slot(page, n) ((page) + UNIV_PAGE_SIZE - PAGE_DIR - (n + 1) * PAGE_DIR_SLOT_SIZE)
#endif /* UNIV_DEBUG */

/**
 * Looks for the directory slot which owns the given record.
 * 
 * @param[in]rec the physical record
 * 
 * @return	the directory slot number
 */
ulint page_dir_find_owner_slot(const rec_t *rec);

/**
 * This is a low-level operation which is used in a database index creation
 * to update the page number of a created B-tree to a data dictionary
 * record.
 * @param[in] rec record to update
 * @param[in] i index of the field to update
 * @param[in] page_no value to write
 * @param[im,out] mtr mini-transaction
 */
void page_rec_write_index_page_no(rec_t *rec, ulint i, ulint page_no, mtr_t *mtr);

/**
 * Allocates a block of memory from the heap of an index page.
 * 
 * @param[in,out] page index page
 * @param[in] need total number of bytes needed
 * @param[out] heap_no heap number
 * 
 * @return	pointer to start of allocated buffer, or NULL if allocation fails */
byte *page_mem_alloc_heap(page_t *page, ulint need, ulint *heap_no);

/**
 * Create a B-tree index page.
 * 
 * @param[in,out] block buffer block where the page is created
 * @param[in,out] mtr mini-transaction
 * 
 * @return	pointer to the page
 */
page_t *page_create(buf_block_t *block, mtr_t *mtr);

/**
 * Differs from page_copy_rec_list_end, because this function does not
 * touch the lock table and max trx id on page.
 * 
 * @param[in,out] new_block index page to copy to
 * @param[in] block index page of rec
 * @param[in] rec record on page
 * @param[in] index index that contains the record
 * @param[in,out] mtr mini-transaction
 */
void page_copy_rec_list_end_no_locks(
  buf_block_t *new_block,
  buf_block_t *block,
  rec_t *rec,
  dict_index_t *index,
  mtr_t *mtr
);

/**
 * Copies records from page to new_page, from the given record onward,
 * including that record. Infimum and supremum records are not copied.
 * The records are copied to the start of the record list on new_page.
 * 
 * @param[in, out] new_block index page to copy to
 * @param[in] block index page containing rec
 * @param[in] rec record on page
 * @param[in] index containing the page
 * @param[in, out] mtr mini-transaction
 * @return pointer to the original successor of the infimum record on new_page.
 */
rec_t *page_copy_rec_list_end(
  buf_block_t *new_block,
  buf_block_t *block,
  rec_t *rec,
  dict_index_t *index,
  mtr_t *mtr
);

/**
 * Copies records from page to new_page, up to the given record, NOT
 * including that record. Infimum and supremum records are not copied.
 * The records are copied to the end of the record list on new_page.
 * 
 * @param[in,out] new_block index page to copy to
 * @param[in] block index page containing rec
 * @param[in] rec record on page
 * @param[in] index containing the page
 * @param[in,out] mtr mini-transaction
 * 
 * @return pointer to the original predecessor of the supremum record on new_page
 */
rec_t *page_copy_rec_list_start(
  buf_block_t *new_block,
  buf_block_t *block,
  rec_t *rec,
  dict_index_t *index,
  mtr_t *mtr
);

/**
 * Deletes records from a page from a given record onward, including that
 * record. The infimum and supremum records are not deleted.
 * 
 * @param[in] rec record on page
 * @param[in] block buffer block of the page
 * @param[in] index record descriptor
 * @param[in] n_recs number of records to delete, or ULINT_UNDEFINED if not known
 * @param[in] size the sum of the sizes of the records in the end of the chain to delete, or ULINT_UNDEFINED if not known
 * @param[in,out] mtr mini-transaction
 */
void page_delete_rec_list_end(
  rec_t *rec,
  buf_block_t *block,
  dict_index_t *index,
  ulint n_recs,
  ulint size,
  mtr_t *mtr
);

/**
 * Deletes records from page, up to the given record, NOT including
 * that record. Infimum and supremum records are not deleted.
 * 
 * @param[in] rec record on page
 * @param[in] block buffer block of the page
 * @param[in] index containing the record
 * @param[in,out] mtr mini-transaction
 */
void page_delete_rec_list_start(rec_t *rec, buf_block_t *block, dict_index_t *index, mtr_t *mtr);

/**
 * Moves record list end to another page. Moved records include split_rec.
 * 
 * @param[in,out] new_block index page where to move
 * @param[in] block index page from where to move
 * @param[in] split_rec first record to move
 * @param[in] index that owns the record
 * @param[in,out] mtr mini-transaction
 * 
 * @return true on success
 */
bool page_move_rec_list_end(
  buf_block_t *new_block,
  buf_block_t *block,
  rec_t *split_rec,
  dict_index_t *index,
  mtr_t *mtr
);

/**
 * Moves record list start to another page. Moved records do not include
 * split_rec.
 * 
 * @param[in,out] new_block index page where to move
 * @param[in] block index page from where to move
 * @param[in] split_rec first record not to move
 * @param[in] index that owns the record
 * @param[in,out] mtr mini-transaction
 * 
 * @return	true on success
 */
bool page_move_rec_list_start(
  buf_block_t *new_block,
  buf_block_t *block,
  rec_t *split_rec,
  dict_index_t *index,
  mtr_t *mtr
);

/**
 * Splits a directory slot which owns too many records.
 * 
 * @param[in] page index page
 * @param[in] slot_no the directory slot
 */
void page_dir_split_slot(page_t *page, ulint slot_no);

/**
 * Tries to balance the given directory slot with too few records
 * with the upper neighbor, so that there are at least the minimum number
 * of records owned by the slot; this may result in the merging of
 * two slots.
 * 
 * @param[in] page index page
 * @param[in] slot_no the directory slot
 */
void page_dir_balance_slot(page_t *page, ulint slot_no);

/**
 * Parses a log record of a record list end or start deletion.
 * 
 * @param[in] type MLOG_LIST_END_DELETE, MLOG_LIST_START_DELETE,
 * @param[in] ptr buffer
 * @param[in] end_ptr buffer end
 * @param[in,out] block buffer block or NULL
 * @param[in] index that owns the record
 * 
 * @return	end of log record or NULL
 */
byte *page_parse_delete_rec_list(
  byte type,
  byte *ptr,
  byte *end_ptr,
  buf_block_t *block,
  dict_index_t *index,
  mtr_t *mtr
);

/**
 * Parses a redo log record of creating a page.
 * 
 * @param[in] ptr buffer
 * @param[in] end_ptr buffer end
 * @param[in,out] block buffer block or NULL
 * @param[im,out] mtr mini-transaction
 * 
 * @return	end of log record or NULL
 */
byte *page_parse_create(byte *ptr, byte *end_ptr, buf_block_t *block, mtr_t *mtr);

/**
 * Prints record contents including the data relevant only in
 * the index page context.
 * 
 * @param[in] rec physical record
 * @param[in] offsets array returned by Phy_rec::get_col_offsets()
 */
void page_rec_print(const rec_t *rec, const ulint *offsets);

/**
 * This is used to print the contents of the directory for
 * debugging purposes.
 * 
 * @param[in] page index page
 * @param[in] pr_n print n first and n last entries
 */
void page_dir_print(page_t *page, ulint pr_n);

/**
 * This is used to print the contents of the page record list for
 * debugging purposes.
 * 
 * @param[in] block index page
 * @param[in] index dictionary index of the page
 * @param[in] pr_n print n first and last entries in directory
 */
void page_print_list(buf_block_t *block, dict_index_t *index, ulint pr_n);

/**
 * Prints the info in a page header.
 * 
 * @param[in] page index page
 */
void page_header_print(const page_t *page);

/**
 * This is used to print the contents of the page for
 * debugging purposes.
 * 
 * @param[in] block index page
 * @param[in] index dictionary index of the page
 * @param[in] dn print dn first and last entries in directory
 * @param[in] rn print rn first and last records in directory
 */
void page_print(buf_block_t *block, dict_index_t *index, ulint dn, ulint rn);

/**
 * The following is used to validate a record on a page. This function
 * differs from rec_validate as it can also check the n_owned field and
 * the heap_no field.
 * 
 * @param[in] rec physical record
 * @param[in] offsets array returned by Phy_rec::get_col_offsets()
 * @return	true if ok */
bool page_rec_validate(rec_t *rec, const ulint *offsets);

/**
 * Checks that the first directory slot points to the infimum record and
 * the last to the supremum. This function is intended to track if the
 * bug fixed in 4.0.14 has caused corruption to users' databases.
 * 
 * @param[in] page index page
 */
void page_check_dir(const page_t *page);

/**
 * This function checks the consistency of an index page when we do not
 * know the index. This is also resilient so that this should never crash
 * even if the page is total garbage.
 * 
 * @param[in] page index page
 * 
 * @return	true if ok
 */
bool page_simple_validate(page_t *page);

/**
 * This function checks the consistency of an index page.
 * 
 * @param[in] page index page
 * @param[in] index data dictionary index containing the page record type definition
 * 
 * @return	true if ok
 */
bool page_validate(page_t *page, dict_index_t *index);

/**
 * Looks in the page record list for a record with the given heap number.
 * 
 * @param[in] page index page
 * @param[in] heap_no heap number
 * 
 * @return	record, NULL if not found
 */
const rec_t *page_find_rec_with_heap_no(const page_t *page, ulint heap_no);

/**
 * Determine if a record is so big that it needs to be stored externally.
 * 
 * @param[in] rec_size length of the record in bytes
 * 
 * @return	false if the entire record can be stored locally on the page
 */
inline bool page_rec_needs_ext(ulint rec_size);

/**
 * Gets the start of a page.
 * 
 * @param[in] ptr pointer to page frame
 * 
 * @return	start of the page
 */
inline page_t *page_align(const void *ptr)
{
  return reinterpret_cast<page_t *>(ut_align_down(ptr, UNIV_PAGE_SIZE));
}

/**
 * Gets the offset within a page.
 * 
 * @param[in] ptr pointer to page frame
 * 
 * @return	offset from the start of the page
 */
inline ulint page_offset(const void *ptr) {
  return ut_align_offset(ptr, UNIV_PAGE_SIZE);
}

/**
 * @param[in] page page to read from 
 * 
 * @eturns the max trx id field value.
 */
inline trx_id_t page_get_max_trx_id(const page_t *page) {
  return mach_read_from_8(page + PAGE_HEADER + PAGE_MAX_TRX_ID);
}

/**
 * Sets the max trx id field value if trx_id is bigger than the previous
 * value.
 * 
 * @param[in,out] block page
 * @param[in] trx_id transaction id
 * @param[in,out] mtr mini-transaction
 */
inline void page_update_max_trx_id(buf_block_t *block, trx_id_t trx_id, mtr_t *mtr) {
  ut_ad(mtr->memo_contains(block, MTR_MEMO_PAGE_X_FIX));

  /* During crash recovery, this function may be called on
  something else than a leaf page of a secondary index returns
  true for the dummy indexes constructed during redo log
  application).  In that case, PAGE_MAX_TRX_ID is unused,
  and trx_id is usually zero. */
  ut_ad(trx_id > 0 || recv_recovery_on);
  ut_ad(page_is_leaf(block->get_frame()));

  if (page_get_max_trx_id(block->get_frame()) < trx_id) {

    page_set_max_trx_id(block, trx_id, mtr);
  }
}

/**
 * Sets the given header field.
 * 
 * @param[in,out] page page to write to
 * @param[in] field field to write within the page, PAGE_N_DIR_SLOTS, ...
 * @param[in] val value to write
 */
inline void page_header_set_field(page_t *page, ulint field, ulint val) {
  ut_ad(field <= PAGE_N_RECS);
  ut_ad(field == PAGE_N_HEAP || val < UNIV_PAGE_SIZE);
  ut_ad(field != PAGE_N_HEAP || (val & 0x7fff) < UNIV_PAGE_SIZE);

  mach_write_to_2(page + PAGE_HEADER + field, val);
}

/**
 * Set header pointer
 * @param page page to write to
 * @param field field to write to, PAGE_FREE, PAGE_LAST_INSERT, PAGE_HEAP_TOP
 * @param ptr pointer to write, or nullptr
 */
inline void page_header_set_ptr(page_t *page, ulint field, const byte *ptr) {
  ut_ad((field == PAGE_FREE) || (field == PAGE_LAST_INSERT) || (field == PAGE_HEAP_TOP));

  ulint offs = ptr == nullptr ? 0 : ptr - page;

  ut_ad((field != PAGE_HEAP_TOP) || offs);

  page_header_set_field(page, field, offs);
}

/**
 * Resets the last insert info field in the page header. Writes to mlog
 * about this operation.
 * 
 * @param[in,out] page Page to write to
 * @param[in,out] mtr mini-transaction
 */
inline void page_header_reset_last_insert(page_t *page, mtr_t *mtr) {
  mlog_write_ulint(page + (PAGE_HEADER + PAGE_LAST_INSERT), 0, MLOG_2BYTES, mtr);
}

/**
 * Returns the heap number of a record.
 * 
 * @param[in] rec physical record
 * 
 * @return	heap number
 */
inline ulint page_rec_get_heap_no(const rec_t *rec) {
  return rec_get_heap_no(rec);
}

/**
 * Gets the offset of the first record on the page.
 * 
 * @param[in] page page which must have record(s)
 * 
 * @return	offset of the first record in record list, relative from page
 */
inline constexpr ulint page_get_infimum_offset(const page_t *page) {
  ut_ad(!page_offset(page));

  return PAGE_INFIMUM;
}

/**
 * Gets the offset of the last record on the page.
 * 
 * @param[in] page page which must have record(s)
 * 
 * @return	offset of the last record in record list, relative from page
 */
inline constexpr ulint page_get_supremum_offset(const page_t *page) /*!< in: page which must have record(s) */
{
  ut_ad(!page_offset(page));

  return PAGE_SUPREMUM;
}

/**
 * Returns the offset of the first record on the page.
 * 
 * @param[in] page page which must have record(s)
 * 
 * @return	true if a user record
 */
inline bool page_rec_is_user_rec_low(ulint offset) {
  ut_ad(offset <= UNIV_PAGE_SIZE - PAGE_EMPTY_DIR_START);

  return likely(offset != PAGE_INFIMUM) && likely(offset != PAGE_SUPREMUM);
}

/**
 * true if the record is the supremum record on a page.
 * 
 * @param[in] offset record offset on page
 * 
 * @return	true if the supremum record
 */
inline bool page_rec_is_supremum_low(ulint offset) {
  ut_ad(offset <= UNIV_PAGE_SIZE - PAGE_EMPTY_DIR_START);

  return unlikely(offset == PAGE_SUPREMUM);
}

/**
 * true if the record is the infimum record on a page.
 * 
 * @param[in] offset record offset on page
 * 
 * @return	true if the infimum record
 */
inline bool page_rec_is_infimum_low(ulint offset) {
  ut_ad(offset <= UNIV_PAGE_SIZE - PAGE_EMPTY_DIR_START);

  return unlikely(offset == PAGE_INFIMUM);
}

/**
 * true if the record is a user record on the page.
 * 
 * @param[in] rec record
 * 
 * @return	true if a user record
 */
inline bool page_rec_is_user_rec(const rec_t *rec) {
  return page_rec_is_user_rec_low(page_offset(rec));
}

/**
 * true if the record is the supremum record on a page.
 * 
 * @param[in] rec record
 * 
 * @return	true if the supremum record
 */
inline bool page_rec_is_supremum(const rec_t *rec) {
  return page_rec_is_supremum_low(page_offset(rec));
}

/**
 * true if the record is the infimum record on a page.
 * 
 * @param[in] rec record
 * 
 * @return	true if the infimum record
 */
inline bool page_rec_is_infimum(const rec_t *rec) {
  return page_rec_is_infimum_low(page_offset(rec));
}

/**
 * Compares a data tuple to a physical record. Differs from the function
 * cmp_dtuple_rec_with_match in the way that the record must reside on an
 * index page, and also page infimum and supremum records can be given in
 * the parameter rec. These are considered as the negative infinity and
 * the positive infinity in the alphabetical order.
 * 
 * @param[in] cmp_ctx client compare context
 * @param[in] dtuple data tuple
 * @param[in] rec physical record on a page; may also be page infimum or supremum,
 *  in which case matched-parameter values below are not affected
 * @param[in] offsets array returned by Phy_rec::get_col_offsets()
 * @param[in,out] matched_fields number of already completely matched fields;
 *  when function returns contains the value for current comparison
 * @param[in,out] matched_bytes number of already matched bytes within the first
 *  field not completely matched; when function returns contains the value for
 *  current comparison
 * 
 * @return 1, 0, -1, if dtuple is greater, equal, less than rec,
 *  respectively, when only the common first fields are compared
 */
inline int page_cmp_dtuple_rec_with_match(
  void *cmp_ctx,
  const dtuple_t *dtuple,
  const rec_t *rec,
  const ulint *offsets,
  ulint *matched_fields,
  ulint *matched_bytes
) {
  ut_ad(dtuple_check_typed(dtuple));
  ut_ad(rec_offs_validate(rec, nullptr, offsets));

  const auto rec_offset = page_offset(rec);

  if (unlikely(rec_offset == PAGE_INFIMUM)) {
    return 1;
  }

  if (unlikely(rec_offset == PAGE_SUPREMUM)) {
    return -1;
  }

  return cmp_dtuple_rec_with_match(cmp_ctx, dtuple, rec, offsets, matched_fields, matched_bytes);
}

/**
 * Gets the page number.
 * 
 * @param[in] page page to read the number from
 * 
 * @return	page number
 */
inline ulint page_get_page_no(const page_t *page) {
  ut_ad(page == page_align((page_t *)page));
  return mach_read_from_4(page + FIL_PAGE_OFFSET);
}

/**
 * Gets the tablespace identifier.
 * 
 * @param[in] page page to read the space id from
 * 
 * @return	space id
 */
inline ulint page_get_space_id(const page_t *page) {
  ut_ad(page == page_align((page_t *)page));

  return mach_read_from_4(page + FIL_PAGE_SPACE_ID);
}

/**
 * Gets the number of user records on page (infimum and supremum records
 * are not user records).
 * 
 * @param[in] page index page
 * 
 * @return	number of user records
 */
inline ulint page_get_n_recs(const page_t *page) {
  return page_header_get_field(page, PAGE_N_RECS);
}

/**
 * Gets the number of dir slots in directory.
 * 
 * @param[in] page index page
 * 
 * @return	number of slots
 */
inline ulint page_dir_get_n_slots(const page_t *page) {
  return page_header_get_field(page, PAGE_N_DIR_SLOTS);
}

/**
 * Sets the number of dir slots in directory.
 * 
 * @param[in] page index page
 */
inline void page_dir_set_n_slots(page_t *page, ulint n_slots) {
  page_header_set_field(page, PAGE_N_DIR_SLOTS, n_slots);
}

/**
 * Gets the number of records in the heap.
 * 
 * @param[in] page index page
 * 
 * @return	number of user records
 */
inline ulint page_dir_get_n_heap(const page_t *page) {
  return page_header_get_field(page, PAGE_N_HEAP) & 0x7fff;
}

/**
 * Sets the number of records in the heap.
 * 
 * @param[in] page index page
 * @param[in] n_heap number of records
 */
inline void page_dir_set_n_heap(page_t *page, ulint n_heap) {
  ut_ad(n_heap < 0x8000);

  page_header_set_field(page, PAGE_N_HEAP, n_heap | (0x8000 & page_header_get_field(page, PAGE_N_HEAP)));
}

#ifdef UNIV_DEBUG
/**
 * Gets pointer to nth directory slot.
 * 
 * @param[in] page index page
 * @param[in] n position
 * @return	pointer to dir slot
 */
inline page_dir_slot_t *page_dir_get_nth_slot(const page_t *page, ulint n) {
  ut_ad(page_dir_get_n_slots(page) > n);

  return (page_dir_slot_t *)page + UNIV_PAGE_SIZE - PAGE_DIR - (n + 1) * PAGE_DIR_SLOT_SIZE;
}
#endif /* UNIV_DEBUG */

/**
 * Used to check the consistency of a record on a page.
 * 
 * @param[in] rec record
 * 
 * @return	true if succeed
 */
inline bool page_rec_check(const rec_t *rec) {
  const page_t *page = page_align(rec);

  ut_a(rec);

  ut_a(page_offset(rec) <= page_header_get_field(page, PAGE_HEAP_TOP));
  ut_a(page_offset(rec) >= PAGE_DATA);

  return true;
}

/**
 * Gets the record pointed to by a directory slot.
 * 
 * @param[in] slot directory slot
 * 
 * @return	pointer to record
 */
inline const rec_t *page_dir_slot_get_rec(const page_dir_slot_t *slot) {
  return page_align(slot) + mach_read_from_2(slot);
}

/**
 * This is used to set the record offset in a directory slot.
 * 
 * @param[in] slot directory slot
 * @param[in] rec record on the page
 * 
 */
inline void page_dir_slot_set_rec(page_dir_slot_t *slot, rec_t *rec) {
  ut_ad(page_rec_check(rec));

  mach_write_to_2(slot, page_offset(rec));
}

/**
 * Gets the number of records owned by a directory slot.
 * 
 * @param[in] slot directory slot
 * 
 * @return	number of records
 */
inline ulint page_dir_slot_get_n_owned(const page_dir_slot_t *slot) {
  const rec_t *rec = page_dir_slot_get_rec(slot);

  return rec_get_n_owned(rec);
}

/**
 * This is used to set the owned records field of a directory slot.
 * 
 * @param[in] slot directory slot
 * @param[in] n number of records owned by the slot
 */
inline void page_dir_slot_set_n_owned(page_dir_slot_t *slot, ulint n) {
  auto ptr = const_cast<byte*>(page_dir_slot_get_rec(slot));
  auto rec = reinterpret_cast<rec_t*>(ptr);

  rec_set_n_owned(rec, n);
}

/**
 * Calculates the space reserved for directory slots of a given number of
 * records. The exact value is a fraction number n * PAGE_DIR_SLOT_SIZE /
 * PAGE_DIR_SLOT_MIN_N_OWNED, and it is rounded upwards to an integer.
 * 
 * @param[in] n_recs number of records
 */
inline ulint page_dir_calc_reserved_space(ulint n_recs) {
  return (PAGE_DIR_SLOT_SIZE * n_recs + PAGE_DIR_SLOT_MIN_N_OWNED - 1) / PAGE_DIR_SLOT_MIN_N_OWNED;
}

/**
 * Gets the pointer to the next record on the page.
 * 
 * @param[in] rec pointer to record
 * 
 * @return	pointer to next record
 */
inline const rec_t *page_rec_get_next_low(const rec_t *rec) {
  ut_ad(page_rec_check(rec));

  auto page = page_align(rec);
  auto offs = rec_get_next_offs(rec);

  if (unlikely(offs >= UNIV_PAGE_SIZE)) {
    log_err(std::format(
      "Next record offset is nonsensical {} in record at offset {}"
      "rec address {}, space id {}, page {}",
      offs,
      page_offset(rec),
      (void *)rec,
      page_get_space_id(page),
      page_get_page_no(page)
    ));

    buf_page_print(page, 0);

    ut_error;
  }

  return offs == 0 ? nullptr : page + offs;
}

/**
 * Gets the pointer to the next record on the page.
 * 
 * @param[in] rec pointer to record
 * 
 * @return	pointer to next record
 */
inline rec_t *page_rec_get_next(rec_t *rec) {
  auto ptr = const_cast<rec_t *>(page_rec_get_next_low(rec));
  return reinterpret_cast<rec_t *>(ptr);
}

/**
 * Gets the pointer to the next record on the page.
 * 
 * @param[in] rec pointer to record
 * 
 * @return	pointer to next record
 */
inline const rec_t *page_rec_get_next_const(const rec_t *rec) {
  return page_rec_get_next_low(rec);
}

/**
 * Sets the pointer to the next record on the page.
 * 
 * @param[in] rec pointer to record, must not be page supremum
 * @param[in] next pointer to next record, must not be page infimum
 */
inline void page_rec_set_next(rec_t *rec, rec_t *next) {
  ut_ad(page_rec_check(rec));
  ut_ad(!page_rec_is_supremum(rec));
  ut_ad(rec != next);

  ut_ad(next == nullptr || !page_rec_is_infimum(next));
  ut_ad(next == nullptr || page_align(rec) == page_align(next));

  auto offs = likely(next != nullptr) ? page_offset(next) : 0;

  rec_set_next_offs(rec, offs);
}

/**
 * Gets the pointer to the previous record.
 * 
 * @param[in] rec pointer to record, must not be page infimum
 * 
 * @return	pointer to previous record
 */
inline const rec_t *page_rec_get_prev_const(const rec_t *rec) {
  ut_ad(page_rec_check(rec));

  auto page = page_align(rec);

  ut_ad(!page_rec_is_infimum(rec));

  auto slot_no = page_dir_find_owner_slot(rec);

  ut_a(slot_no != 0);

  auto slot = page_dir_get_nth_slot(page, slot_no - 1);

  auto rec2 = page_dir_slot_get_rec(slot);

  const rec_t *prev_rec{};

  while (rec != rec2) {
    prev_rec = rec2;
    rec2 = page_rec_get_next_low(rec2);
  }

  ut_a(prev_rec != nullptr);

  return prev_rec;
}

/**
 * Gets the pointer to the previous record.
 * 
 * @param[in] rec pointer to record, must not be page infimum
 * 
 * @return	pointer to previous record
 */
inline rec_t *page_rec_get_prev(rec_t *rec) {
  auto ptr = const_cast<byte*>(page_rec_get_prev_const(rec));
  return reinterpret_cast<rec_t *>(ptr);
}

/**
 * Looks for the record which owns the given record.
 * 
 * @param[in] rec the physical record
 * 
 * @return	the owner record
 */
inline rec_t *page_rec_find_owner_rec(rec_t *rec) {
  ut_ad(page_rec_check(rec));

  while (rec_get_n_owned(rec) == 0) {
    rec = page_rec_get_next(rec);
  }

  return rec;
}

/**
 * Returns the base extra size of a physical record.  This is the
 * size of the fixed header, independent of the record size.
 * 
 * @param[in] rec physical record
 * 
 * @return	REC_N_EXTRA_BYTES
 */
constexpr inline ulint page_rec_get_base_extra_size(const rec_t *rec) {
  return REC_N_EXTRA_BYTES;
}

/**
 * Returns the sum of the sizes of the records in the record list, excluding
 * the infimum and supremum records.
 * 
 * @param[in] page index page
 * 
 * @return	data in bytes
 */
inline ulint page_get_data_size(const page_t *page) {
  const auto ret = static_cast<ulint>(page_header_get_field(page, PAGE_HEAP_TOP) - PAGE_SUPREMUM_END - page_header_get_field(page, PAGE_GARBAGE));

  ut_ad(ret < UNIV_PAGE_SIZE);

  return ret;
}

/**
 * Allocates a block of memory from the free list of an index page.
 * 
 * @param[in,out] page index page
 * @param[in] next_rec pointer to the new head of the free record list
 * @param[in] need number of bytes allocated
 */
inline void page_mem_alloc_free(page_t *page, rec_t *next_rec, ulint need) {
#ifdef UNIV_DEBUG
  auto old_rec = page_header_get_ptr(page, PAGE_FREE);

  ut_ad(old_rec != nullptr);
  auto next_offs = rec_get_next_offs(old_rec);
  ut_ad(next_rec == (next_offs > 0 ? page + next_offs : nullptr));
#endif /* UNIV_DEBUG */

  page_header_set_ptr(page, PAGE_FREE, next_rec);

  auto garbage = page_header_get_field(page, PAGE_GARBAGE);

  ut_ad(garbage >= need);

  page_header_set_field(page, PAGE_GARBAGE, garbage - need);
}

/**
 * Calculates free space if a page is emptied.
 * 
 * @return	free space
 */
constexpr inline ulint page_get_free_space_of_empty() {
  return static_cast<ulint>(UNIV_PAGE_SIZE - PAGE_SUPREMUM_END - PAGE_DIR - 2 * PAGE_DIR_SLOT_SIZE);
}

/**
 * Each user record on a page, and also the deleted user records in the heap
 * takes its size plus the fraction of the dir cell size /
 * PAGE_DIR_SLOT_MIN_N_OWNED bytes for it. If the sum of these exceeds the
 * value of page_get_free_space_of_empty, the insert is impossible, otherwise
 * it is allowed. This function returns the maximum combined size of records
 * which can be inserted on top of the record heap.
 * 
 * @param[in] page index page
 * @param[in] n_recs number of records to insert
 * 
 * @return	maximum combined size for inserted records
 */
inline ulint page_get_max_insert_size(const page_t *page, ulint n_recs) {
  const auto occupied = page_header_get_field(page, PAGE_HEAP_TOP) - PAGE_SUPREMUM_END +
    page_dir_calc_reserved_space(n_recs + page_dir_get_n_heap(page) - 2);

  const auto free_space = page_get_free_space_of_empty();

  /* Above the 'n_recs +' part reserves directory space for the new
  inserted records; the '- 2' excludes page infimum and supremum
  records */

  return occupied > free_space ? 0 : free_space - occupied;
}

/**
 * Returns the maximum combined size of records which can be inserted on top
 * of the record heap if a page is first reorganized.
 * 
 * @param[in] page index page
 * @param[in] n_recs number of records to insert
 * 
 * @return	maximum combined size for inserted records
 */
inline ulint page_get_max_insert_size_after_reorganize(const page_t *page, ulint n_recs) {
  const auto occupied = page_get_data_size(page) + page_dir_calc_reserved_space(n_recs + page_get_n_recs(page));
  const auto free_space = page_get_free_space_of_empty();

  return occupied > free_space ? 0 : free_space - occupied;
}

/**
 * Puts a record to free list.
 * 
 * @param[in,out] page index page
 * @param[in] rec pointer to the record
 * @param[in] dict_index index of rec
 * @param[in] offsets array returned by Phy_rec::get_col_offsets()
 */
inline void page_mem_free(page_t *page, rec_t *rec, dict_index_t *dict_index, const ulint *offsets) {
  ut_ad(rec_offs_validate(rec, dict_index, offsets));

  auto free_rec = page_header_get_ptr(page, PAGE_FREE);

  page_rec_set_next(rec, free_rec);
  page_header_set_ptr(page, PAGE_FREE, rec);

  const auto garbage = page_header_get_field(page, PAGE_GARBAGE);

  page_header_set_field(page, PAGE_GARBAGE, garbage + rec_offs_size(offsets));

  page_header_set_field(page, PAGE_N_RECS, page_get_n_recs(page) - 1);
}

/** 
 * Check if a record needs to be stored externally.
 * 
 * @return true if the record needs to be stored externally
 */
inline bool page_rec_needs_ext(ulint rec_size) {
  ut_ad(rec_size > REC_N_EXTRA_BYTES);

#if UNIV_PAGE_SIZE > REC_MAX_DATA_SIZE
  if (unlikely(rec_size >= REC_MAX_DATA_SIZE)) {
    return true;
  }
#endif /* UNIV_PAGE_SIZE > REC_MAX_DATA_SIZE */

  return rec_size >= page_get_free_space_of_empty() / 2;
}
