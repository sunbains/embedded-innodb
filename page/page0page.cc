/****************************************************************************
Copyright (c) 1994, 2010, Innobase Oy. All Rights Reserved.

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

/** @file page/page0page.c
Index page routines

Created 2/2/1994 Heikki Tuuri
*******************************************************/

#include "page0page.h"

#include "btr0btr.h"

#include "buf0buf.h"
#include "fut0lst.h"
#include "lock0lock.h"
#include "page0cur.h"
#include "srv0srv.h"

/*			THE INDEX PAGE
                        ==============

The index page consists of a page header which contains the page's
id and other information. On top of it are the index records
in a heap linked into a one way linear list according to alphabetic order.

Just below page end is an array of pointers which we call page directory,
to about every sixth record in the list. The pointers are placed in
the directory in the alphabetical order of the records pointed to,
enabling us to make binary search using the array. Each slot n:o I
in the directory points to a record, where a 4-bit field contains a count
of those records which are in the linear list between pointer I and
the pointer I - 1 in the directory, including the record
pointed to by pointer I and not including the record pointed to by I - 1.
We say that the record pointed to by slot I, or that slot I, owns
these records. The count is always kept in the range 4 to 8, with
the exception that it is 1 for the first slot, and 1--8 for the second slot.

An essentially binary search can be performed in the list of index
records, like we could do if we had pointer to every record in the
page directory. The data structure is, however, more efficient when
we are doing inserts, because most inserts are just pushed on a heap.
Only every 8th insert requires block move in the directory pointer
table, which itself is quite small. A record is deleted from the page
by just taking it off the linear list and updating the number of owned
records-field of the record which owns it, and updating the page directory,
if necessary. A special case is the one when the record owns itself.
Because the overhead of inserts is so small, we may also increase the
page size from the projected default of 8 kB to 64 kB without too
much loss of efficiency in inserts. Bigger page becomes actual
when the disk transfer rate compared to seek and latency time rises.
On the present system, the page size is set so that the page transfer
time (3 ms) is 20 % of the disk random access time (15 ms).

When the page is split, merged, or becomes full but contains deleted
records, we have to reorganize the page.

Assuming a page size of 8 kB, a typical index page of a secondary
index contains 300 index entries, and the size of the page directory
is 50 x 4 bytes = 200 bytes. */

ulint page_dir_find_owner_slot(const rec_t *rec) {
  const page_t *page;
  uint16_t rec_offs_bytes;
  const page_dir_slot_t *slot;
  const page_dir_slot_t *first_slot;
  const rec_t *r = rec;

  ut_ad(page_rec_check(rec));

  page = page_align(rec);
  first_slot = page_dir_get_nth_slot(page, 0);
  slot = page_dir_get_nth_slot(page, page_dir_get_n_slots(page) - 1);

  if (page_is_comp(page)) {
    while (rec_get_n_owned_new(r) == 0) {
      r = rec_get_next_ptr_const(r, true);
      ut_ad(r >= page + PAGE_NEW_SUPREMUM);
      ut_ad(r < page + (UNIV_PAGE_SIZE - PAGE_DIR));
    }
  } else {
    while (rec_get_n_owned_old(r) == 0) {
      r = rec_get_next_ptr_const(r, false);
      ut_ad(r >= page + PAGE_OLD_SUPREMUM);
      ut_ad(r < page + (UNIV_PAGE_SIZE - PAGE_DIR));
    }
  }

  rec_offs_bytes = mach_encode_2(r - page);

  while (likely(*(uint16_t *)slot != rec_offs_bytes)) {

    if (unlikely(slot == first_slot)) {
      ib_logger(
        ib_stream,
        "Probable data corruption on"
        " page %lu\n"
        "Original record ",
        (ulong)page_get_page_no(page)
      );

      if (page_is_comp(page)) {
        ib_logger(ib_stream, "(compact record)");
      } else {
        rec_print_old(ib_stream, rec);
      }

      ib_logger(
        ib_stream,
        "\n"
        "on that page.\n"
        "Cannot find the dir slot for record "
      );
      if (page_is_comp(page)) {
        ib_logger(ib_stream, "(compact record)");
      } else {
        rec_print_old(ib_stream, page + mach_decode_2(rec_offs_bytes));
      }
      ib_logger(
        ib_stream,
        "\n"
        "on that page!\n"
      );

      buf_page_print(page, 0);

      ut_error;
    }

    slot += PAGE_DIR_SLOT_SIZE;
  }

  return (((ulint)(first_slot - slot)) / PAGE_DIR_SLOT_SIZE);
}

/** Used to check the consistency of a directory slot.
@return	true if succeed */
static bool page_dir_slot_check(page_dir_slot_t *slot) /*!< in: slot */
{
  page_t *page;
  ulint n_slots;
  ulint n_owned;

  ut_a(slot);

  page = page_align(slot);

  n_slots = page_dir_get_n_slots(page);

  ut_a(slot <= page_dir_get_nth_slot(page, 0));
  ut_a(slot >= page_dir_get_nth_slot(page, n_slots - 1));

  ut_a(page_rec_check(page_dir_slot_get_rec(slot)));

  if (page_is_comp(page)) {
    n_owned = rec_get_n_owned_new(page_dir_slot_get_rec(slot));
  } else {
    n_owned = rec_get_n_owned_old(page_dir_slot_get_rec(slot));
  }

  if (slot == page_dir_get_nth_slot(page, 0)) {
    ut_a(n_owned == 1);
  } else if (slot == page_dir_get_nth_slot(page, n_slots - 1)) {
    ut_a(n_owned >= 1);
    ut_a(n_owned <= PAGE_DIR_SLOT_MAX_N_OWNED);
  } else {
    ut_a(n_owned >= PAGE_DIR_SLOT_MIN_N_OWNED);
    ut_a(n_owned <= PAGE_DIR_SLOT_MAX_N_OWNED);
  }

  return (true);
}

void page_set_max_trx_id(buf_block_t *block, trx_id_t trx_id, mtr_t *mtr) {
  auto page = block->get_frame();

  ut_ad(!mtr || mtr_memo_contains(mtr, block, MTR_MEMO_PAGE_X_FIX));

  /* It is not necessary to write this change to the redo log, as
  during a database recovery we assume that the max trx id of every
  page is the maximum trx id assigned before the crash. */

  if (mtr != nullptr) {
    mlog_write_uint64(page + (PAGE_HEADER + PAGE_MAX_TRX_ID), trx_id, mtr);
  } else {
    mach_write_to_8(page + (PAGE_HEADER + PAGE_MAX_TRX_ID), trx_id);
  }
}

byte *page_mem_alloc_heap(page_t *page, ulint need, ulint *heap_no) {
  ut_ad(page && heap_no);

  auto avl_space = page_get_max_insert_size(page, 1);

  if (avl_space >= need) {
    auto block = page_header_get_ptr(page, PAGE_HEAP_TOP);

    page_header_set_ptr(page, PAGE_HEAP_TOP, block + need);

    *heap_no = page_dir_get_n_heap(page);

    page_dir_set_n_heap(page, 1 + *heap_no);

    return block;
  }

  return (nullptr);
}

/** Writes a log record of page creation. */
inline void page_create_write_log(
  buf_frame_t *frame, /*!< in: a buffer frame where the
                                          page is created */
  mtr_t *mtr,         /*!< in: mini-transaction handle */
  bool comp
) /*!< in: true=compact page format */
{
  mlog_write_initial_log_record(frame, comp ? MLOG_COMP_PAGE_CREATE : MLOG_PAGE_CREATE, mtr);
}

byte *page_parse_create(byte *ptr, byte *, ulint comp, buf_block_t *block, mtr_t *mtr) {
  ut_ad(ptr != nullptr);

  /* The record is empty, except for the record initial part */

  if (block != nullptr) {
    page_create(block, mtr, comp);
  }

  return ptr;
}

/** The index page creation function.
@return	pointer to the page */
static page_t *page_create_low(
  buf_block_t *block, /*!< in: a buffer block where the
                                    page is created */
  ulint comp
) /*!< in: nonzero=compact page format */
{
  page_dir_slot_t *slot;
  mem_heap_t *heap;
  dtuple_t *tuple;
  dfield_t *field;
  byte *heap_top;
  rec_t *infimum_rec;
  rec_t *supremum_rec;
  page_t *page;
  dict_index_t *index;
  ulint *offsets;

  ut_ad(block != nullptr);

  /* The infimum and supremum records use a dummy index. */
  if (likely(comp)) {
    index = dict_ind_compact;
  } else {
    index = dict_ind_redundant;
  }

  /* 1. INCREMENT MODIFY CLOCK */
  buf_block_modify_clock_inc(block);

  page = block->get_frame();

  srv_fil->page_set_type(page, FIL_PAGE_INDEX);

  heap = mem_heap_create(200);

  /* 3. CREATE THE INFIMUM AND SUPREMUM RECORDS */

  /* Create first a data tuple for infimum record */
  tuple = dtuple_create(heap, 1);
  dtuple_set_info_bits(tuple, REC_STATUS_INFIMUM);
  field = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(field, "infimum", 8);
  dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH | DATA_NOT_NULL, 8);
  /* Set the corresponding physical record to its place in the page
  record heap */

  heap_top = page + PAGE_DATA;

  infimum_rec = rec_convert_dtuple_to_rec(heap_top, index, tuple, 0);

  if (likely(comp)) {
    ut_a(infimum_rec == page + PAGE_NEW_INFIMUM);

    rec_set_n_owned_new(infimum_rec, 1);
    rec_set_heap_no_new(infimum_rec, 0);
  } else {
    ut_a(infimum_rec == page + PAGE_OLD_INFIMUM);

    rec_set_n_owned_old(infimum_rec, 1);
    rec_set_heap_no_old(infimum_rec, 0);
  }

  offsets = rec_get_offsets(infimum_rec, index, nullptr, ULINT_UNDEFINED, &heap);

  heap_top = rec_get_end(infimum_rec, offsets);

  /* Create then a tuple for supremum */

  tuple = dtuple_create(heap, 1);
  dtuple_set_info_bits(tuple, REC_STATUS_SUPREMUM);
  field = dtuple_get_nth_field(tuple, 0);

  dfield_set_data(field, "supremum", comp ? 8 : 9);
  dtype_set(dfield_get_type(field), DATA_VARCHAR, DATA_ENGLISH | DATA_NOT_NULL, comp ? 8 : 9);

  supremum_rec = rec_convert_dtuple_to_rec(heap_top, index, tuple, 0);

  if (likely(comp)) {
    ut_a(supremum_rec == page + PAGE_NEW_SUPREMUM);

    rec_set_n_owned_new(supremum_rec, 1);
    rec_set_heap_no_new(supremum_rec, 1);
  } else {
    ut_a(supremum_rec == page + PAGE_OLD_SUPREMUM);

    rec_set_n_owned_old(supremum_rec, 1);
    rec_set_heap_no_old(supremum_rec, 1);
  }

  offsets = rec_get_offsets(supremum_rec, index, offsets, ULINT_UNDEFINED, &heap);
  heap_top = rec_get_end(supremum_rec, offsets);

  ut_ad(heap_top == page + (comp ? PAGE_NEW_SUPREMUM_END : PAGE_OLD_SUPREMUM_END));

  mem_heap_free(heap);

  /* 4. INITIALIZE THE PAGE */

  page_header_set_field(page, PAGE_N_DIR_SLOTS, 2);
  page_header_set_ptr(page, PAGE_HEAP_TOP, heap_top);
  page_header_set_field(page, PAGE_N_HEAP, comp ? 0x8000 | PAGE_HEAP_NO_USER_LOW : PAGE_HEAP_NO_USER_LOW);
  page_header_set_ptr(page, PAGE_FREE, nullptr);
  page_header_set_field(page, PAGE_GARBAGE, 0);
  page_header_set_ptr(page, PAGE_LAST_INSERT, nullptr);
  page_header_set_field(page, PAGE_DIRECTION, PAGE_NO_DIRECTION);
  page_header_set_field(page, PAGE_N_DIRECTION, 0);
  page_header_set_field(page, PAGE_N_RECS, 0);
  page_set_max_trx_id(block, 0, nullptr);
  memset(heap_top, 0, UNIV_PAGE_SIZE - PAGE_EMPTY_DIR_START - page_offset(heap_top));

  /* 5. SET POINTERS IN RECORDS AND DIR SLOTS */

  /* Set the slots to point to infimum and supremum. */

  slot = page_dir_get_nth_slot(page, 0);
  page_dir_slot_set_rec(slot, infimum_rec);

  slot = page_dir_get_nth_slot(page, 1);
  page_dir_slot_set_rec(slot, supremum_rec);

  /* Set the next pointers in infimum and supremum */

  if (likely(comp)) {
    rec_set_next_offs_new(infimum_rec, PAGE_NEW_SUPREMUM);
    rec_set_next_offs_new(supremum_rec, 0);
  } else {
    rec_set_next_offs_old(infimum_rec, PAGE_OLD_SUPREMUM);
    rec_set_next_offs_old(supremum_rec, 0);
  }

  return (page);
}

page_t *page_create(buf_block_t *block, mtr_t *mtr, ulint comp) {
  page_create_write_log(block->get_frame(), mtr, comp);
  return (page_create_low(block, comp));
}

void page_copy_rec_list_end_no_locks(buf_block_t *new_block, buf_block_t *block, rec_t *rec, dict_index_t *index, mtr_t *mtr) {
  page_t *new_page = new_block->get_frame();
  page_cur_t cur1;
  rec_t *cur2;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  page_cur_position(rec, block, &cur1);

  if (page_cur_is_before_first(&cur1)) {

    page_cur_move_to_next(&cur1);
  }

  ut_a((bool)!!page_is_comp(new_page) == dict_table_is_comp(index->table));
  ut_a(page_is_comp(new_page) == page_rec_is_comp(rec));
  ut_a(mach_read_from_2(new_page + UNIV_PAGE_SIZE - 10) == (ulint)(page_is_comp(new_page) ? PAGE_NEW_INFIMUM : PAGE_OLD_INFIMUM));

  cur2 = page_get_infimum_rec(new_block->get_frame());

  /* Copy records from the original page to the new page */

  while (!page_cur_is_after_last(&cur1)) {
    rec_t *cur1_rec = page_cur_get_rec(&cur1);
    rec_t *ins_rec;
    offsets = rec_get_offsets(cur1_rec, index, offsets, ULINT_UNDEFINED, &heap);
    ins_rec = page_cur_insert_rec_low(cur2, index, cur1_rec, offsets, mtr);
    if (unlikely(!ins_rec)) {
      /* Track an assertion failure reported on the mailing
      list on June 18th, 2003 */

      buf_page_print(new_page, 0);
      buf_page_print(page_align(rec), 0);
      ut_print_timestamp(ib_stream);

      ib_logger(
        ib_stream,
        "rec offset %lu, cur1 offset %lu,"
        " cur2 offset %lu\n",
        (ulong)page_offset(rec),
        (ulong)page_offset(page_cur_get_rec(&cur1)),
        (ulong)page_offset(cur2)
      );
      ut_error;
    }

    page_cur_move_to_next(&cur1);
    cur2 = ins_rec;
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

rec_t *page_copy_rec_list_end(buf_block_t *new_block, buf_block_t *block, rec_t *rec, dict_index_t *index, mtr_t *mtr) {
  page_t *new_page = new_block->get_frame();
  page_t *page = page_align(rec);
  rec_t *ret = page_rec_get_next(page_get_infimum_rec(new_page));

  ut_ad(block->get_frame() == page);
  ut_ad(page_is_leaf(page) == page_is_leaf(new_page));
  ut_ad(page_is_comp(page) == page_is_comp(new_page));

  /* Here, "ret" may be pointing to a user record or the
  predefined supremum record. */

  if (page_dir_get_n_heap(new_page) == PAGE_HEAP_NO_USER_LOW) {
    page_copy_rec_list_end_to_created_page(new_page, rec, index, mtr);
  } else {
    page_copy_rec_list_end_no_locks(new_block, block, rec, index, mtr);
  }

  if (dict_index_is_sec(index) && page_is_leaf(page)) {
    page_update_max_trx_id(new_block, page_get_max_trx_id(page), mtr);
  }

  /* Update the lock table and possible hash index */

  lock_move_rec_list_end(new_block, block, rec);

  return ret;
}

rec_t *page_copy_rec_list_start(buf_block_t *new_block, buf_block_t *block, rec_t *rec, dict_index_t *index, mtr_t *mtr) {
  page_t *new_page = new_block->get_frame();
  page_cur_t cur1;
  rec_t *cur2;
  mem_heap_t *heap = nullptr;
  rec_t *ret = page_rec_get_prev(page_get_supremum_rec(new_page));
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  /* Here, "ret" may be pointing to a user record or the
  predefined infimum record. */

  if (page_rec_is_infimum(rec)) {

    return (ret);
  }

  page_cur_set_before_first(block, &cur1);
  page_cur_move_to_next(&cur1);

  cur2 = ret;

  /* Copy records from the original page to the new page */

  while (page_cur_get_rec(&cur1) != rec) {
    rec_t *cur1_rec = page_cur_get_rec(&cur1);
    offsets = rec_get_offsets(cur1_rec, index, offsets, ULINT_UNDEFINED, &heap);
    cur2 = page_cur_insert_rec_low(cur2, index, cur1_rec, offsets, mtr);
    ut_a(cur2);

    page_cur_move_to_next(&cur1);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  if (dict_index_is_sec(index) && page_is_leaf(page_align(rec))) {
    page_update_max_trx_id(new_block, page_get_max_trx_id(page_align(rec)), mtr);
  }

  /* Update the lock table and possible hash index */

  lock_move_rec_list_start(new_block, block, rec, ret);

  return ret;
}

/** Writes a log record of a record list end or start deletion. */
inline void page_delete_rec_list_write_log(
  rec_t *rec,          /*!< in: record on page */
  dict_index_t *index, /*!< in: record descriptor */
  byte type,           /*!< in: operation type:
                         MLOG_LIST_END_DELETE, ... */
  mtr_t *mtr
) /*!< in: mtr */
{
  byte *log_ptr;
  ut_ad(
    type == MLOG_LIST_END_DELETE || type == MLOG_LIST_START_DELETE || type == MLOG_COMP_LIST_END_DELETE ||
    type == MLOG_COMP_LIST_START_DELETE
  );

  log_ptr = mlog_open_and_write_index(mtr, rec, index, type, 2);
  if (log_ptr) {
    /* Write the parameter as a 2-byte ulint */
    mach_write_to_2(log_ptr, page_offset(rec));
    mlog_close(mtr, log_ptr + 2);
  }
}

byte *page_parse_delete_rec_list(byte type, byte *ptr, byte *end_ptr, buf_block_t *block, dict_index_t *index, mtr_t *mtr) {
  page_t *page;
  ulint offset;

  ut_ad(
    type == MLOG_LIST_END_DELETE || type == MLOG_LIST_START_DELETE || type == MLOG_COMP_LIST_END_DELETE ||
    type == MLOG_COMP_LIST_START_DELETE
  );

  /* Read the record offset as a 2-byte ulint */

  if (end_ptr < ptr + 2) {

    return (nullptr);
  }

  offset = mach_read_from_2(ptr);
  ptr += 2;

  if (!block) {

    return (ptr);
  }

  page = block->get_frame();

  ut_ad(!!page_is_comp(page) == dict_table_is_comp(index->table));

  if (type == MLOG_LIST_END_DELETE || type == MLOG_COMP_LIST_END_DELETE) {
    page_delete_rec_list_end(page + offset, block, index, ULINT_UNDEFINED, ULINT_UNDEFINED, mtr);
  } else {
    page_delete_rec_list_start(page + offset, block, index, mtr);
  }

  return (ptr);
}

void page_delete_rec_list_end(rec_t *rec, buf_block_t *block, dict_index_t *index, ulint n_recs, ulint size, mtr_t *mtr) {
  page_dir_slot_t *slot;
  ulint slot_index;
  rec_t *last_rec;
  rec_t *prev_rec;
  ulint n_owned;
  page_t *page = page_align(rec);
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(size == ULINT_UNDEFINED || size < UNIV_PAGE_SIZE);

  if (page_rec_is_infimum(rec)) {
    rec = page_rec_get_next(rec);
  }

  if (page_rec_is_supremum(rec)) {

    return;
  }

  /* Reset the last insert info in the page header and increment
  the modify clock for the frame */

  page_header_set_ptr(page, PAGE_LAST_INSERT, nullptr);

  /* The page gets invalid for optimistic searches: increment the
  frame modify clock */

  buf_block_modify_clock_inc(block);

  page_delete_rec_list_write_log(rec, index, page_is_comp(page) ? MLOG_COMP_LIST_END_DELETE : MLOG_LIST_END_DELETE, mtr);

  prev_rec = page_rec_get_prev(rec);

  last_rec = page_rec_get_prev(page_get_supremum_rec(page));

  if ((size == ULINT_UNDEFINED) || (n_recs == ULINT_UNDEFINED)) {
    rec_t *rec2 = rec;
    /* Calculate the sum of sizes and the number of records */
    size = 0;
    n_recs = 0;

    do {
      ulint s;
      offsets = rec_get_offsets(rec2, index, offsets, ULINT_UNDEFINED, &heap);
      s = rec_offs_size(offsets);
      ut_ad(rec2 - page + s - rec_offs_extra_size(offsets) < UNIV_PAGE_SIZE);
      ut_ad(size + s < UNIV_PAGE_SIZE);
      size += s;
      n_recs++;

      rec2 = page_rec_get_next(rec2);
    } while (!page_rec_is_supremum(rec2));

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
  }

  ut_ad(size < UNIV_PAGE_SIZE);

  /* Update the page directory; there is no need to balance the number
  of the records owned by the supremum record, as it is allowed to be
  less than PAGE_DIR_SLOT_MIN_N_OWNED */

  if (page_is_comp(page)) {
    rec_t *rec2 = rec;
    ulint count = 0;

    while (rec_get_n_owned_new(rec2) == 0) {
      count++;

      rec2 = rec_get_next_ptr(rec2, true);
    }

    ut_ad(rec_get_n_owned_new(rec2) > count);

    n_owned = rec_get_n_owned_new(rec2) - count;
    slot_index = page_dir_find_owner_slot(rec2);
    slot = page_dir_get_nth_slot(page, slot_index);
  } else {
    rec_t *rec2 = rec;
    ulint count = 0;

    while (rec_get_n_owned_old(rec2) == 0) {
      count++;

      rec2 = rec_get_next_ptr(rec2, false);
    }

    ut_ad(rec_get_n_owned_old(rec2) > count);

    n_owned = rec_get_n_owned_old(rec2) - count;
    slot_index = page_dir_find_owner_slot(rec2);
    slot = page_dir_get_nth_slot(page, slot_index);
  }

  page_dir_slot_set_rec(slot, page_get_supremum_rec(page));
  page_dir_slot_set_n_owned(slot, n_owned);

  page_dir_set_n_slots(page, slot_index + 1);

  /* Remove the record chain segment from the record chain */
  page_rec_set_next(prev_rec, page_get_supremum_rec(page));

  /* Catenate the deleted chain segment to the page free list */

  page_rec_set_next(last_rec, page_header_get_ptr(page, PAGE_FREE));
  page_header_set_ptr(page, PAGE_FREE, rec);

  page_header_set_field(page, PAGE_GARBAGE, size + page_header_get_field(page, PAGE_GARBAGE));

  page_header_set_field(page, PAGE_N_RECS, (ulint)(page_get_n_recs(page) - n_recs));
}

void page_delete_rec_list_start(rec_t *rec, buf_block_t *block, dict_index_t *index, mtr_t *mtr) {
  page_cur_t cur1;
  ulint log_mode;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  mem_heap_t *heap = nullptr;
  byte type;

  rec_offs_init(offsets_);

  ut_ad((bool)!!page_rec_is_comp(rec) == dict_table_is_comp(index->table));

  if (page_rec_is_infimum(rec)) {

    return;
  }

  if (page_rec_is_comp(rec)) {
    type = MLOG_COMP_LIST_START_DELETE;
  } else {
    type = MLOG_LIST_START_DELETE;
  }

  page_delete_rec_list_write_log(rec, index, type, mtr);

  page_cur_set_before_first(block, &cur1);
  page_cur_move_to_next(&cur1);

  /* Individual deletes are not logged */

  log_mode = mtr_set_log_mode(mtr, MTR_LOG_NONE);

  while (page_cur_get_rec(&cur1) != rec) {
    offsets = rec_get_offsets(page_cur_get_rec(&cur1), index, offsets, ULINT_UNDEFINED, &heap);
    page_cur_delete_rec(&cur1, index, offsets, mtr);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  /* Restore log mode */

  mtr_set_log_mode(mtr, log_mode);
}

bool page_move_rec_list_end(buf_block_t *new_block, buf_block_t *block, rec_t *split_rec, dict_index_t *index, mtr_t *mtr) {
  page_t *new_page = new_block->get_frame();
  ulint old_data_size;
  ulint new_data_size;
  ulint old_n_recs;
  ulint new_n_recs;

  old_data_size = page_get_data_size(new_page);
  old_n_recs = page_get_n_recs(new_page);

  if (unlikely(!page_copy_rec_list_end(new_block, block, split_rec, index, mtr))) {
    return false;
  }

  new_data_size = page_get_data_size(new_page);
  new_n_recs = page_get_n_recs(new_page);

  ut_ad(new_data_size >= old_data_size);

  page_delete_rec_list_end(split_rec, block, index, new_n_recs - old_n_recs, new_data_size - old_data_size, mtr);

  return (true);
}

bool page_move_rec_list_start(buf_block_t *new_block, buf_block_t *block, rec_t *split_rec, dict_index_t *index, mtr_t *mtr) {
  if (unlikely(!page_copy_rec_list_start(new_block, block, split_rec, index, mtr))) {
    return (false);
  }

  page_delete_rec_list_start(split_rec, block, index, mtr);

  return (true);
}

void page_rec_write_index_page_no(rec_t *rec, ulint i, ulint page_no, mtr_t *mtr) {
  byte *data;
  ulint len;

  data = rec_get_nth_field_old(rec, i, &len);

  ut_ad(len == 4);

  mlog_write_ulint(data, page_no, MLOG_4BYTES, mtr);
}

/** Used to delete n slots from the directory. This function updates
also n_owned fields in the records, so that the first slot after
the deleted ones inherits the records of the deleted slots. */
inline void page_dir_delete_slot(
  page_t *page, /*!< in/out: the index page */
  ulint slot_no
) /*!< in: slot to be deleted */
{
  page_dir_slot_t *slot;
  ulint n_owned;
  ulint i;
  ulint n_slots;

  ut_ad(slot_no > 0);
  ut_ad(slot_no + 1 < page_dir_get_n_slots(page));

  n_slots = page_dir_get_n_slots(page);

  /* 1. Reset the n_owned fields of the slots to be
  deleted */
  slot = page_dir_get_nth_slot(page, slot_no);
  n_owned = page_dir_slot_get_n_owned(slot);
  page_dir_slot_set_n_owned(slot, 0);

  /* 2. Update the n_owned value of the first non-deleted slot */

  slot = page_dir_get_nth_slot(page, slot_no + 1);
  page_dir_slot_set_n_owned(slot, n_owned + page_dir_slot_get_n_owned(slot));

  /* 3. Destroy the slot by copying slots */
  for (i = slot_no + 1; i < n_slots; i++) {
    rec_t *rec = (rec_t *)page_dir_slot_get_rec(page_dir_get_nth_slot(page, i));
    page_dir_slot_set_rec(page_dir_get_nth_slot(page, i - 1), rec);
  }

  /* 4. Zero out the last slot, which will be removed */
  mach_write_to_2(page_dir_get_nth_slot(page, n_slots - 1), 0);

  /* 5. Update the page header */
  page_header_set_field(page, PAGE_N_DIR_SLOTS, n_slots - 1);
}

/** Used to add n slots to the directory. Does not set the record pointers
in the added slots or update n_owned values: this is the responsibility
of the caller. */
inline void page_dir_add_slot(
  page_t *page, /*!< in/out: the index page */
  ulint start
) /*!< in: the slot above which the new
                                           slots are added */
{
  page_dir_slot_t *slot;
  ulint n_slots;

  n_slots = page_dir_get_n_slots(page);

  ut_ad(start < n_slots - 1);

  /* Update the page header */
  page_dir_set_n_slots(page, n_slots + 1);

  /* Move slots up */
  slot = page_dir_get_nth_slot(page, n_slots);
  memmove(slot, slot + PAGE_DIR_SLOT_SIZE, (n_slots - 1 - start) * PAGE_DIR_SLOT_SIZE);
}

void page_dir_split_slot(page_t *page, ulint slot_no) {
  rec_t *rec;
  page_dir_slot_t *new_slot;
  page_dir_slot_t *prev_slot;
  page_dir_slot_t *slot;
  ulint i;
  ulint n_owned;

  ut_ad(page);
  ut_ad(slot_no > 0);

  slot = page_dir_get_nth_slot(page, slot_no);

  n_owned = page_dir_slot_get_n_owned(slot);
  ut_ad(n_owned == PAGE_DIR_SLOT_MAX_N_OWNED + 1);

  /* 1. We loop to find a record approximately in the middle of the
  records owned by the slot. */

  prev_slot = page_dir_get_nth_slot(page, slot_no - 1);
  rec = (rec_t *)page_dir_slot_get_rec(prev_slot);

  for (i = 0; i < n_owned / 2; i++) {
    rec = page_rec_get_next(rec);
  }

  ut_ad(n_owned / 2 >= PAGE_DIR_SLOT_MIN_N_OWNED);

  /* 2. We add one directory slot immediately below the slot to be
  split. */

  page_dir_add_slot(page, slot_no - 1);

  /* The added slot is now number slot_no, and the old slot is
  now number slot_no + 1 */

  new_slot = page_dir_get_nth_slot(page, slot_no);
  slot = page_dir_get_nth_slot(page, slot_no + 1);

  /* 3. We store the appropriate values to the new slot. */

  page_dir_slot_set_rec(new_slot, rec);
  page_dir_slot_set_n_owned(new_slot, n_owned / 2);

  /* 4. Finally, we update the number of records field of the
  original slot */

  page_dir_slot_set_n_owned(slot, n_owned - (n_owned / 2));
}

void page_dir_balance_slot(page_t *page, ulint slot_no) {
  page_dir_slot_t *slot;
  page_dir_slot_t *up_slot;
  ulint n_owned;
  ulint up_n_owned;
  rec_t *old_rec;
  rec_t *new_rec;

  ut_ad(page);
  ut_ad(slot_no > 0);

  slot = page_dir_get_nth_slot(page, slot_no);

  /* The last directory slot cannot be balanced with the upper
  neighbor, as there is none. */

  if (unlikely(slot_no == page_dir_get_n_slots(page) - 1)) {

    return;
  }

  up_slot = page_dir_get_nth_slot(page, slot_no + 1);

  n_owned = page_dir_slot_get_n_owned(slot);
  up_n_owned = page_dir_slot_get_n_owned(up_slot);

  ut_ad(n_owned == PAGE_DIR_SLOT_MIN_N_OWNED - 1);

  /* If the upper slot has the minimum value of n_owned, we will merge
  the two slots, therefore we assert: */
  ut_ad(2 * PAGE_DIR_SLOT_MIN_N_OWNED - 1 <= PAGE_DIR_SLOT_MAX_N_OWNED);

  if (up_n_owned > PAGE_DIR_SLOT_MIN_N_OWNED) {

    /* In this case we can just transfer one record owned
    by the upper slot to the property of the lower slot */
    old_rec = (rec_t *)page_dir_slot_get_rec(slot);

    if (page_is_comp(page)) {
      new_rec = rec_get_next_ptr(old_rec, true);

      rec_set_n_owned_new(old_rec, 0);
      rec_set_n_owned_new(new_rec, n_owned + 1);
    } else {
      new_rec = rec_get_next_ptr(old_rec, false);

      rec_set_n_owned_old(old_rec, 0);
      rec_set_n_owned_old(new_rec, n_owned + 1);
    }

    page_dir_slot_set_rec(slot, new_rec);

    page_dir_slot_set_n_owned(up_slot, up_n_owned - 1);
  } else {
    /* In this case we may merge the two slots */
    page_dir_delete_slot(page, slot_no);
  }
}

rec_t *page_get_middle_rec(page_t *page) {
  page_dir_slot_t *slot;
  ulint middle;
  ulint i;
  ulint n_owned;
  ulint count;
  rec_t *rec;

  /* This many records we must leave behind */
  middle = (page_get_n_recs(page) + PAGE_HEAP_NO_USER_LOW) / 2;

  count = 0;

  for (i = 0;; i++) {

    slot = page_dir_get_nth_slot(page, i);
    n_owned = page_dir_slot_get_n_owned(slot);

    if (count + n_owned > middle) {
      break;
    } else {
      count += n_owned;
    }
  }

  ut_ad(i > 0);
  slot = page_dir_get_nth_slot(page, i - 1);
  rec = (rec_t *)page_dir_slot_get_rec(slot);
  rec = page_rec_get_next(rec);

  /* There are now count records behind rec */

  for (i = 0; i < middle - count; i++) {
    rec = page_rec_get_next(rec);
  }

  return (rec);
}

ulint page_rec_get_n_recs_before(const rec_t *rec) {
  const page_dir_slot_t *slot;
  const rec_t *slot_rec;
  const page_t *page;
  ulint i;
  lint n = 0;

  ut_ad(page_rec_check(rec));

  page = page_align(rec);
  if (page_is_comp(page)) {
    while (rec_get_n_owned_new(rec) == 0) {

      rec = rec_get_next_ptr_const(rec, true);
      n--;
    }

    for (i = 0;; i++) {
      slot = page_dir_get_nth_slot(page, i);
      slot_rec = page_dir_slot_get_rec(slot);

      n += rec_get_n_owned_new(slot_rec);

      if (rec == slot_rec) {

        break;
      }
    }
  } else {
    while (rec_get_n_owned_old(rec) == 0) {

      rec = rec_get_next_ptr_const(rec, false);
      n--;
    }

    for (i = 0;; i++) {
      slot = page_dir_get_nth_slot(page, i);
      slot_rec = page_dir_slot_get_rec(slot);

      n += rec_get_n_owned_old(slot_rec);

      if (rec == slot_rec) {

        break;
      }
    }
  }

  n--;

  ut_ad(n >= 0);

  return ((ulint)n);
}

void page_rec_print(const rec_t *rec, const ulint *offsets) {
  ut_a(!page_rec_is_comp(rec) == !rec_offs_comp(offsets));
  rec_print_new(ib_stream, rec, offsets);
  if (page_rec_is_comp(rec)) {
    ib_logger(
      ib_stream,
      " n_owned: %lu; heap_no: %lu; next rec: %lu\n",
      (ulong)rec_get_n_owned_new(rec),
      (ulong)rec_get_heap_no_new(rec),
      (ulong)rec_get_next_offs(rec, true)
    );
  } else {
    ib_logger(
      ib_stream,
      " n_owned: %lu; heap_no: %lu; next rec: %lu\n",
      (ulong)rec_get_n_owned_old(rec),
      (ulong)rec_get_heap_no_old(rec),
      (ulong)rec_get_next_offs(rec, true)
    );
  }

  page_rec_check(rec);
  rec_validate(rec, offsets);
}

void page_dir_print(page_t *page, ulint pr_n) {
  ulint n;
  ulint i;
  page_dir_slot_t *slot;

  n = page_dir_get_n_slots(page);

  ib_logger(
    ib_stream,
    "--------------------------------\n"
    "PAGE DIRECTORY\n"
    "Page address %p\n"
    "Directory stack top at offs: %lu; number of slots: %lu\n",
    page,
    (ulong)page_offset(page_dir_get_nth_slot(page, n - 1)),
    (ulong)n
  );
  for (i = 0; i < n; i++) {
    slot = page_dir_get_nth_slot(page, i);
    if ((i == pr_n) && (i < n - pr_n)) {
      ib_logger(ib_stream, "    ...   \n");
    }
    if ((i < pr_n) || (i >= n - pr_n)) {
      ib_logger(
        ib_stream,
        "Contents of slot: %lu: n_owned: %lu,"
        " rec offs: %lu\n",
        (ulong)i,
        (ulong)page_dir_slot_get_n_owned(slot),
        (ulong)page_offset(page_dir_slot_get_rec(slot))
      );
    }
  }
  ib_logger(
    ib_stream,
    "Total of %lu records\n"
    "--------------------------------\n",
    (ulong)(PAGE_HEAP_NO_USER_LOW + page_get_n_recs(page))
  );
}

void page_print_list(buf_block_t *block, dict_index_t *index, ulint pr_n) {
  page_t *page = block->m_frame;
  page_cur_t cur;
  ulint count;
  ulint n_recs;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_a((bool)!!page_is_comp(page) == dict_table_is_comp(index->table));

  ib_logger(
    ib_stream,
    "--------------------------------\n"
    "PAGE RECORD LIST\n"
    "Page address %p\n",
    page
  );

  n_recs = page_get_n_recs(page);

  page_cur_set_before_first(block, &cur);
  count = 0;
  for (;;) {
    offsets = rec_get_offsets(cur.m_rec, index, offsets, ULINT_UNDEFINED, &heap);
    page_rec_print(cur.m_rec, offsets);

    if (count == pr_n) {
      break;
    }
    if (page_cur_is_after_last(&cur)) {
      break;
    }
    page_cur_move_to_next(&cur);
    count++;
  }

  if (n_recs > 2 * pr_n) {
    ib_logger(ib_stream, " ... \n");
  }

  while (!page_cur_is_after_last(&cur)) {
    page_cur_move_to_next(&cur);

    if (count + pr_n >= n_recs) {
      offsets = rec_get_offsets(cur.m_rec, index, offsets, ULINT_UNDEFINED, &heap);
      page_rec_print(cur.m_rec, offsets);
    }
    count++;
  }

  ib_logger(
    ib_stream,
    "Total of %lu records \n"
    "--------------------------------\n",
    (ulong)(count + 1)
  );

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

void page_header_print(const page_t *page) {
  ib_logger(
    ib_stream,
    "--------------------------------\n"
    "PAGE HEADER INFO\n"
    "Page address %p, n records %lu (%s)\n"
    "n dir slots %lu, heap top %lu\n"
    "Page n heap %lu, free %lu, garbage %lu\n"
    "Page last insert %lu, direction %lu, n direction %lu\n",
    page,
    (ulong)page_header_get_field(page, PAGE_N_RECS),
    page_is_comp(page) ? "compact format" : "original format",
    (ulong)page_header_get_field(page, PAGE_N_DIR_SLOTS),
    (ulong)page_header_get_field(page, PAGE_HEAP_TOP),
    (ulong)page_dir_get_n_heap(page),
    (ulong)page_header_get_field(page, PAGE_FREE),
    (ulong)page_header_get_field(page, PAGE_GARBAGE),
    (ulong)page_header_get_field(page, PAGE_LAST_INSERT),
    (ulong)page_header_get_field(page, PAGE_DIRECTION),
    (ulong)page_header_get_field(page, PAGE_N_DIRECTION)
  );
}

void page_print(buf_block_t *block, dict_index_t *index, ulint dn, ulint rn) {
  page_t *page = block->m_frame;

  page_header_print(page);
  page_dir_print(page, dn);
  page_print_list(block, index, rn);
}

bool page_rec_validate(rec_t *rec, const ulint *offsets) {
  ulint n_owned;
  ulint heap_no;
  page_t *page;

  page = page_align(rec);
  ut_a(!page_is_comp(page) == !rec_offs_comp(offsets));

  page_rec_check(rec);
  rec_validate(rec, offsets);

  if (page_rec_is_comp(rec)) {
    n_owned = rec_get_n_owned_new(rec);
    heap_no = rec_get_heap_no_new(rec);
  } else {
    n_owned = rec_get_n_owned_old(rec);
    heap_no = rec_get_heap_no_old(rec);
  }

  if (unlikely(!(n_owned <= PAGE_DIR_SLOT_MAX_N_OWNED))) {
    ib_logger(ib_stream, "Dir slot of rec %lu, n owned too big %lu\n", (ulong)page_offset(rec), (ulong)n_owned);
    return (false);
  }

  if (unlikely(!(heap_no < page_dir_get_n_heap(page)))) {
    ib_logger(
      ib_stream, "Heap no of rec %lu too big %lu %lu\n", (ulong)page_offset(rec), (ulong)heap_no, (ulong)page_dir_get_n_heap(page)
    );
    return (false);
  }

  return (true);
}

void page_check_dir(const page_t *page) {
  ulint n_slots;
  ulint infimum_offs;
  ulint supremum_offs;

  n_slots = page_dir_get_n_slots(page);
  infimum_offs = mach_read_from_2(page_dir_get_nth_slot(page, 0));
  supremum_offs = mach_read_from_2(page_dir_get_nth_slot(page, n_slots - 1));

  if (unlikely(!page_rec_is_infimum_low(infimum_offs))) {

    ib_logger(
      ib_stream,
      "Page directory corruption:"
      " infimum not pointed to\n"
    );
    buf_page_print(page, 0);
  }

  if (unlikely(!page_rec_is_supremum_low(supremum_offs))) {

    ib_logger(
      ib_stream,
      "Page directory corruption:"
      " supremum not pointed to\n"
    );
    buf_page_print(page, 0);
  }
}

bool page_simple_validate_old(page_t *page) {
  page_dir_slot_t *slot;
  ulint slot_no;
  ulint n_slots;
  rec_t *rec;
  byte *rec_heap_top;
  ulint count;
  ulint own_count;
  bool ret = false;

  ut_a(!page_is_comp(page));

  /* Check first that the record heap and the directory do not
  overlap. */

  n_slots = page_dir_get_n_slots(page);

  if (unlikely(n_slots > UNIV_PAGE_SIZE / 4)) {
    ib_logger(ib_stream, "Nonsensical number %lu of page dir slots\n", (ulong)n_slots);

    goto func_exit;
  }

  rec_heap_top = page_header_get_ptr(page, PAGE_HEAP_TOP);

  if (unlikely(rec_heap_top > page_dir_get_nth_slot(page, n_slots - 1))) {

    ib_logger(
      ib_stream,
      "Record heap and dir overlap on a page,"
      " heap top %lu, dir %lu\n",
      (ulong)page_header_get_field(page, PAGE_HEAP_TOP),
      (ulong)page_offset(page_dir_get_nth_slot(page, n_slots - 1))
    );

    goto func_exit;
  }

  /* Validate the record list in a loop checking also that it is
  consistent with the page record directory. */

  count = 0;
  own_count = 1;
  slot_no = 0;
  slot = page_dir_get_nth_slot(page, slot_no);

  rec = page_get_infimum_rec(page);

  for (;;) {
    if (unlikely(rec > rec_heap_top)) {
      ib_logger(
        ib_stream,
        "Record %lu is above"
        " rec heap top %lu\n",
        (ulong)(rec - page),
        (ulong)(rec_heap_top - page)
      );

      goto func_exit;
    }

    if (unlikely(rec_get_n_owned_old(rec))) {
      /* This is a record pointed to by a dir slot */
      if (unlikely(rec_get_n_owned_old(rec) != own_count)) {

        ib_logger(
          ib_stream,
          "Wrong owned count %lu, %lu,"
          " rec %lu\n",
          (ulong)rec_get_n_owned_old(rec),
          (ulong)own_count,
          (ulong)(rec - page)
        );

        goto func_exit;
      }

      if (unlikely(page_dir_slot_get_rec(slot) != rec)) {
        ib_logger(
          ib_stream,
          "Dir slot does not point"
          " to right rec %lu\n",
          (ulong)(rec - page)
        );

        goto func_exit;
      }

      own_count = 0;

      if (!page_rec_is_supremum(rec)) {
        slot_no++;
        slot = page_dir_get_nth_slot(page, slot_no);
      }
    }

    if (page_rec_is_supremum(rec)) {

      break;
    }

    if (unlikely(rec_get_next_offs(rec, false) < FIL_PAGE_DATA || rec_get_next_offs(rec, false) >= UNIV_PAGE_SIZE)) {
      ib_logger(
        ib_stream,
        "Next record offset"
        " nonsensical %lu for rec %lu\n",
        (ulong)rec_get_next_offs(rec, false),
        (ulong)(rec - page)
      );

      goto func_exit;
    }

    count++;

    if (unlikely(count > UNIV_PAGE_SIZE)) {
      ib_logger(
        ib_stream,
        "Page record list appears"
        " to be circular %lu\n",
        (ulong)count
      );
      goto func_exit;
    }

    rec = page_rec_get_next(rec);
    own_count++;
  }

  if (unlikely(rec_get_n_owned_old(rec) == 0)) {
    ib_logger(ib_stream, "n owned is zero in a supremum rec\n");

    goto func_exit;
  }

  if (unlikely(slot_no != n_slots - 1)) {
    ib_logger(ib_stream, "n slots wrong %lu, %lu\n", (ulong)slot_no, (ulong)(n_slots - 1));
    goto func_exit;
  }

  if (unlikely(page_header_get_field(page, PAGE_N_RECS) + PAGE_HEAP_NO_USER_LOW != count + 1)) {
    ib_logger(
      ib_stream,
      "n recs wrong %lu %lu\n",
      (ulong)page_header_get_field(page, PAGE_N_RECS) + PAGE_HEAP_NO_USER_LOW,
      (ulong)(count + 1)
    );

    goto func_exit;
  }

  /* Check then the free list */
  rec = page_header_get_ptr(page, PAGE_FREE);

  while (rec != nullptr) {
    if (unlikely(rec < page + FIL_PAGE_DATA || rec >= page + UNIV_PAGE_SIZE)) {
      ib_logger(
        ib_stream,
        "Free list record has"
        " a nonsensical offset %lu\n",
        (ulong)(rec - page)
      );

      goto func_exit;
    }

    if (unlikely(rec > rec_heap_top)) {
      ib_logger(
        ib_stream,
        "Free list record %lu"
        " is above rec heap top %lu\n",
        (ulong)(rec - page),
        (ulong)(rec_heap_top - page)
      );

      goto func_exit;
    }

    count++;

    if (unlikely(count > UNIV_PAGE_SIZE)) {
      ib_logger(
        ib_stream,
        "Page free list appears"
        " to be circular %lu\n",
        (ulong)count
      );
      goto func_exit;
    }

    rec = page_rec_get_next(rec);
  }

  if (unlikely(page_dir_get_n_heap(page) != count + 1)) {

    ib_logger(ib_stream, "N heap is wrong %lu, %lu\n", (ulong)page_dir_get_n_heap(page), (ulong)(count + 1));

    goto func_exit;
  }

  ret = true;

func_exit:
  return (ret);
}

bool page_simple_validate_new(page_t *page) {
  page_dir_slot_t *slot;
  ulint slot_no;
  ulint n_slots;
  rec_t *rec;
  byte *rec_heap_top;
  ulint count;
  ulint own_count;
  bool ret = false;

  ut_a(page_is_comp(page));

  /* Check first that the record heap and the directory do not
  overlap. */

  n_slots = page_dir_get_n_slots(page);

  if (unlikely(n_slots > UNIV_PAGE_SIZE / 4)) {
    ib_logger(
      ib_stream,
      "Nonsensical number %lu"
      " of page dir slots\n",
      (ulong)n_slots
    );

    goto func_exit;
  }

  rec_heap_top = page_header_get_ptr(page, PAGE_HEAP_TOP);

  if (unlikely(rec_heap_top > page_dir_get_nth_slot(page, n_slots - 1))) {

    ib_logger(
      ib_stream,
      "Record heap and dir overlap on a page,"
      " heap top %lu, dir %lu\n",
      (ulong)page_header_get_field(page, PAGE_HEAP_TOP),
      (ulong)page_offset(page_dir_get_nth_slot(page, n_slots - 1))
    );

    goto func_exit;
  }

  /* Validate the record list in a loop checking also that it is
  consistent with the page record directory. */

  count = 0;
  own_count = 1;
  slot_no = 0;
  slot = page_dir_get_nth_slot(page, slot_no);

  rec = page_get_infimum_rec(page);

  for (;;) {
    if (unlikely(rec > rec_heap_top)) {
      ib_logger(
        ib_stream,
        "Record %lu is above rec"
        " heap top %lu\n",
        (ulong)page_offset(rec),
        (ulong)page_offset(rec_heap_top)
      );

      goto func_exit;
    }

    if (unlikely(rec_get_n_owned_new(rec))) {
      /* This is a record pointed to by a dir slot */
      if (unlikely(rec_get_n_owned_new(rec) != own_count)) {

        ib_logger(
          ib_stream,
          "Wrong owned count %lu, %lu,"
          " rec %lu\n",
          (ulong)rec_get_n_owned_new(rec),
          (ulong)own_count,
          (ulong)page_offset(rec)
        );

        goto func_exit;
      }

      if (unlikely(page_dir_slot_get_rec(slot) != rec)) {
        ib_logger(
          ib_stream,
          "Dir slot does not point"
          " to right rec %lu\n",
          (ulong)page_offset(rec)
        );

        goto func_exit;
      }

      own_count = 0;

      if (!page_rec_is_supremum(rec)) {
        slot_no++;
        slot = page_dir_get_nth_slot(page, slot_no);
      }
    }

    if (page_rec_is_supremum(rec)) {

      break;
    }

    if (unlikely(rec_get_next_offs(rec, true) < FIL_PAGE_DATA || rec_get_next_offs(rec, true) >= UNIV_PAGE_SIZE)) {
      ib_logger(
        ib_stream,
        "Next record offset nonsensical %lu"
        " for rec %lu\n",
        (ulong)rec_get_next_offs(rec, true),
        (ulong)page_offset(rec)
      );

      goto func_exit;
    }

    count++;

    if (unlikely(count > UNIV_PAGE_SIZE)) {
      ib_logger(
        ib_stream,
        "Page record list appears"
        " to be circular %lu\n",
        (ulong)count
      );
      goto func_exit;
    }

    rec = page_rec_get_next(rec);
    own_count++;
  }

  if (unlikely(rec_get_n_owned_new(rec) == 0)) {
    ib_logger(
      ib_stream,
      "n owned is zero"
      " in a supremum rec\n"
    );

    goto func_exit;
  }

  if (unlikely(slot_no != n_slots - 1)) {
    ib_logger(ib_stream, "n slots wrong %lu, %lu\n", (ulong)slot_no, (ulong)(n_slots - 1));
    goto func_exit;
  }

  if (unlikely(page_header_get_field(page, PAGE_N_RECS) + PAGE_HEAP_NO_USER_LOW != count + 1)) {
    ib_logger(
      ib_stream,
      "n recs wrong %lu %lu\n",
      (ulong)page_header_get_field(page, PAGE_N_RECS) + PAGE_HEAP_NO_USER_LOW,
      (ulong)(count + 1)
    );

    goto func_exit;
  }

  /* Check then the free list */
  rec = page_header_get_ptr(page, PAGE_FREE);

  while (rec != nullptr) {
    if (unlikely(rec < page + FIL_PAGE_DATA || rec >= page + UNIV_PAGE_SIZE)) {
      ib_logger(
        ib_stream,
        "Free list record has"
        " a nonsensical offset %lu\n",
        (ulong)page_offset(rec)
      );

      goto func_exit;
    }

    if (unlikely(rec > rec_heap_top)) {
      ib_logger(
        ib_stream,
        "Free list record %lu"
        " is above rec heap top %lu\n",
        (ulong)page_offset(rec),
        (ulong)page_offset(rec_heap_top)
      );

      goto func_exit;
    }

    count++;

    if (unlikely(count > UNIV_PAGE_SIZE)) {
      ib_logger(
        ib_stream,
        "Page free list appears"
        " to be circular %lu\n",
        (ulong)count
      );
      goto func_exit;
    }

    rec = page_rec_get_next(rec);
  }

  if (unlikely(page_dir_get_n_heap(page) != count + 1)) {

    ib_logger(ib_stream, "N heap is wrong %lu, %lu\n", (ulong)page_dir_get_n_heap(page), (ulong)(count + 1));

    goto func_exit;
  }

  ret = true;

func_exit:
  return (ret);
}

bool page_validate(page_t *page, dict_index_t *index) {
  page_dir_slot_t *slot;
  mem_heap_t *heap;
  byte *buf;
  ulint count;
  ulint own_count;
  ulint rec_own_count;
  ulint slot_no;
  ulint data_size;
  rec_t *rec;
  rec_t *old_rec = nullptr;
  ulint offs;
  ulint n_slots;
  bool ret = false;
  ulint i;
  ulint *offsets = nullptr;
  ulint *old_offsets = nullptr;

  if (unlikely((bool)!!page_is_comp(page) != dict_table_is_comp(index->table))) {
    ib_logger(ib_stream, "'compact format' flag mismatch\n");
    goto func_exit2;
  }
  if (page_is_comp(page)) {
    if (unlikely(!page_simple_validate_new(page))) {
      goto func_exit2;
    }
  } else {
    if (unlikely(!page_simple_validate_old(page))) {
      goto func_exit2;
    }
  }

  heap = mem_heap_create(UNIV_PAGE_SIZE + 200);

  /* The following buffer is used to check that the
  records in the page record heap do not overlap */

  buf = mem_heap_zalloc(heap, UNIV_PAGE_SIZE);

  /* Check first that the record heap and the directory do not
  overlap. */

  n_slots = page_dir_get_n_slots(page);

  if (unlikely(!(page_header_get_ptr(page, PAGE_HEAP_TOP) <= page_dir_get_nth_slot(page, n_slots - 1)))) {

    ib_logger(
      ib_stream,
      "Record heap and dir overlap"
      " on space %lu page %lu index %s, %p, %p\n",
      (ulong)page_get_space_id(page),
      (ulong)page_get_page_no(page),
      index->name,
      page_header_get_ptr(page, PAGE_HEAP_TOP),
      page_dir_get_nth_slot(page, n_slots - 1)
    );

    goto func_exit;
  }

  /* Validate the record list in a loop checking also that
  it is consistent with the directory. */
  count = 0;
  data_size = 0;
  own_count = 1;
  slot_no = 0;
  slot = page_dir_get_nth_slot(page, slot_no);

  rec = page_get_infimum_rec(page);

  for (;;) {
    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);

    if (page_is_comp(page) && page_rec_is_user_rec(rec) && unlikely(rec_get_node_ptr_flag(rec) == page_is_leaf(page))) {
      ib_logger(ib_stream, "node_ptr flag mismatch\n");
      goto func_exit;
    }

    if (unlikely(!page_rec_validate(rec, offsets))) {
      goto func_exit;
    }

    /* Check that the records are in the ascending order */
    if (likely(count >= PAGE_HEAP_NO_USER_LOW) && !page_rec_is_supremum(rec)) {
      if (unlikely(1 != cmp_rec_rec(rec, old_rec, offsets, old_offsets, index))) {
        ib_logger(
          ib_stream,
          "Records in wrong order"
          " on space %lu page %lu index %s\n",
          (ulong)page_get_space_id(page),
          (ulong)page_get_page_no(page),
          index->name
        );
        ib_logger(ib_stream, "\nprevious record ");
        rec_print_new(ib_stream, old_rec, old_offsets);
        ib_logger(ib_stream, "\nrecord ");
        rec_print_new(ib_stream, rec, offsets);
        ib_logger(ib_stream, "\n");

        goto func_exit;
      }
    }

    if (page_rec_is_user_rec(rec)) {

      data_size += rec_offs_size(offsets);
    }

    offs = page_offset(rec_get_start(rec, offsets));
    i = rec_offs_size(offsets);
    if (unlikely(offs + i >= UNIV_PAGE_SIZE)) {
      ib_logger(ib_stream, "record offset out of bounds\n");
      goto func_exit;
    }

    while (i--) {
      if (unlikely(buf[offs + i])) {
        /* No other record may overlap this */

        ib_logger(ib_stream, "Record overlaps another\n");
        goto func_exit;
      }

      buf[offs + i] = 1;
    }

    if (page_is_comp(page)) {
      rec_own_count = rec_get_n_owned_new(rec);
    } else {
      rec_own_count = rec_get_n_owned_old(rec);
    }

    if (unlikely(rec_own_count)) {
      /* This is a record pointed to by a dir slot */
      if (unlikely(rec_own_count != own_count)) {
        ib_logger(ib_stream, "Wrong owned count %lu, %lu\n", (ulong)rec_own_count, (ulong)own_count);
        goto func_exit;
      }

      if (page_dir_slot_get_rec(slot) != rec) {
        ib_logger(
          ib_stream,
          "Dir slot does not"
          " point to right rec\n"
        );
        goto func_exit;
      }

      page_dir_slot_check(slot);

      own_count = 0;
      if (!page_rec_is_supremum(rec)) {
        slot_no++;
        slot = page_dir_get_nth_slot(page, slot_no);
      }
    }

    if (page_rec_is_supremum(rec)) {
      break;
    }

    count++;
    own_count++;
    old_rec = rec;
    rec = page_rec_get_next(rec);

    /* set old_offsets to offsets; recycle offsets */
    {
      ulint *offs = old_offsets;
      old_offsets = offsets;
      offsets = offs;
    }
  }

  if (page_is_comp(page)) {
    if (unlikely(rec_get_n_owned_new(rec) == 0)) {

      goto n_owned_zero;
    }
  } else if (unlikely(rec_get_n_owned_old(rec) == 0)) {
  n_owned_zero:
    ib_logger(ib_stream, "n owned is zero\n");
    goto func_exit;
  }

  if (unlikely(slot_no != n_slots - 1)) {
    ib_logger(ib_stream, "n slots wrong %lu %lu\n", (ulong)slot_no, (ulong)(n_slots - 1));
    goto func_exit;
  }

  if (unlikely(page_header_get_field(page, PAGE_N_RECS) + PAGE_HEAP_NO_USER_LOW != count + 1)) {
    ib_logger(
      ib_stream,
      "n recs wrong %lu %lu\n",
      (ulong)page_header_get_field(page, PAGE_N_RECS) + PAGE_HEAP_NO_USER_LOW,
      (ulong)(count + 1)
    );
    goto func_exit;
  }

  if (unlikely(data_size != page_get_data_size(page))) {
    ib_logger(ib_stream, "Summed data size %lu, returned by func %lu\n", (ulong)data_size, (ulong)page_get_data_size(page));
    goto func_exit;
  }

  /* Check then the free list */
  rec = page_header_get_ptr(page, PAGE_FREE);

  while (rec != nullptr) {
    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);
    if (unlikely(!page_rec_validate(rec, offsets))) {

      goto func_exit;
    }

    count++;
    offs = page_offset(rec_get_start(rec, offsets));
    i = rec_offs_size(offsets);
    if (unlikely(offs + i >= UNIV_PAGE_SIZE)) {
      ib_logger(ib_stream, "record offset out of bounds\n");
      goto func_exit;
    }

    while (i--) {

      if (unlikely(buf[offs + i])) {
        ib_logger(
          ib_stream,
          "Record overlaps another"
          " in free list\n"
        );
        goto func_exit;
      }

      buf[offs + i] = 1;
    }

    rec = page_rec_get_next(rec);
  }

  if (unlikely(page_dir_get_n_heap(page) != count + 1)) {
    ib_logger(ib_stream, "N heap is wrong %lu %lu\n", (ulong)page_dir_get_n_heap(page), (ulong)count + 1);
    goto func_exit;
  }

  ret = true;

func_exit:
  mem_heap_free(heap);

  if (unlikely(ret == false)) {
  func_exit2:
    ib_logger(
      ib_stream,
      "Apparent corruption"
      " in space %lu page %lu index %s\n",
      (ulong)page_get_space_id(page),
      (ulong)page_get_page_no(page),
      index->name
    );
    buf_page_print(page, 0);
  }

  return (ret);
}

const rec_t *page_find_rec_with_heap_no(const page_t *page, ulint heap_no) {
  const rec_t *rec;

  if (page_is_comp(page)) {
    rec = page + PAGE_NEW_INFIMUM;

    for (;;) {
      ulint rec_heap_no = rec_get_heap_no_new(rec);

      if (rec_heap_no == heap_no) {

        return (rec);
      } else if (rec_heap_no == PAGE_HEAP_NO_SUPREMUM) {

        return (nullptr);
      }

      rec = page + rec_get_next_offs(rec, true);
    }
  } else {
    rec = page + PAGE_OLD_INFIMUM;

    for (;;) {
      ulint rec_heap_no = rec_get_heap_no_old(rec);

      if (rec_heap_no == heap_no) {

        return (rec);
      } else if (rec_heap_no == PAGE_HEAP_NO_SUPREMUM) {

        return (nullptr);
      }

      rec = page + rec_get_next_offs(rec, false);
    }
  }
}
