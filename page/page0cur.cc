/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.

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

/** @file page/page0cur.c
The page cursor

Created 10/4/1994 Heikki Tuuri
*************************************************************************/

#include "innodb0types.h"

#include "log0recv.h"
#include "mtr0log.h"
#include "page0cur.h"
#include "rem0cmp.h"
#include "ut0ut.h"

#ifdef PAGE_CUR_ADAPT
#ifdef UNIV_SEARCH_PERF_STAT
static ulint page_cur_short_succ = 0;
#endif /* UNIV_SEARCH_PERF_STAT */

/** This is a linear congruential generator PRNG. Returns a pseudo random
number between 0 and 2^64-1 inclusive. The formula and the constants
being used are:
X[n+1] = (a * X[n] + c) mod m
where:
X[0] = ut_time_us(nullptr)
a = 1103515245 (3^5 * 5 * 7 * 129749)
c = 12345 (3 * 5 * 823)
m = 18446744073709551616 (2^64)

@return	number between 0 and 2^64-1 */
static uint64_t page_cur_lcg_prng(void) {
#define LCG_a 1103515245
#define LCG_c 12345
  static uint64_t lcg_current = 0;
  static bool initialized = false;

  if (!initialized) {
    lcg_current = (uint64_t)ut_time_us(nullptr);
    initialized = true;
  }

  /* no need to "% 2^64" explicitly because lcg_current is
  64 bit and this will be done anyway */
  lcg_current = LCG_a * lcg_current + LCG_c;

  return (lcg_current);
}

/** Tries a search shortcut based on the last insert.
@return	true on success */
static bool page_cur_try_search_shortcut(
    const buf_block_t *block,  /*!< in: index page */
    const dict_index_t *index, /*!< in: record descriptor */
    const dtuple_t *tuple,     /*!< in: data tuple */
    ulint *iup_matched_fields,
    /*!< in/out: already matched
    fields in upper limit record */
    ulint *iup_matched_bytes,
    /*!< in/out: already matched
    bytes in a field not yet
    completely matched */
    ulint *ilow_matched_fields,
    /*!< in/out: already matched
    fields in lower limit record */
    ulint *ilow_matched_bytes,
    /*!< in/out: already matched
    bytes in a field not yet
    completely matched */
    page_cur_t *cursor) /*!< out: page cursor */
{
  const rec_t *rec;
  const rec_t *next_rec;
  ulint low_match;
  ulint low_bytes;
  ulint up_match;
  ulint up_bytes;
#ifdef UNIV_SEARCH_DEBUG
  page_cur_t cursor2;
#endif
  bool success = false;
  page_t *page = buf_block_get_frame(block);
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(dtuple_check_typed(tuple));

  rec = page_header_get_ptr(page, PAGE_LAST_INSERT);
  offsets =
      rec_get_offsets(rec, index, offsets, dtuple_get_n_fields(tuple), &heap);

  ut_ad(rec);
  ut_ad(page_rec_is_user_rec(rec));

  ut_pair_min(&low_match, &low_bytes, *ilow_matched_fields, *ilow_matched_bytes,
              *iup_matched_fields, *iup_matched_bytes);

  up_match = low_match;
  up_bytes = low_bytes;

  if (page_cmp_dtuple_rec_with_match(index->cmp_ctx, tuple, rec, offsets,
                                     &low_match, &low_bytes) < 0) {

    goto exit_func;
  }

  next_rec = page_rec_get_next_const(rec);
  offsets = rec_get_offsets(next_rec, index, offsets,
                            dtuple_get_n_fields(tuple), &heap);

  if (page_cmp_dtuple_rec_with_match(index->cmp_ctx, tuple, next_rec, offsets,
                                     &up_match, &up_bytes) >= 0) {

    goto exit_func;
  }

  page_cur_position(rec, block, cursor);

#ifdef UNIV_SEARCH_DEBUG
  page_cur_search_with_match(block, index, tuple, PAGE_CUR_DBG,
                             iup_matched_fields, iup_matched_bytes,
                             ilow_matched_fields, ilow_matched_bytes, &cursor2);
  ut_a(cursor2.rec == cursor->rec);

  if (!page_rec_is_supremum(next_rec)) {

    ut_a(*iup_matched_fields == up_match);
    ut_a(*iup_matched_bytes == up_bytes);
  }

  ut_a(*ilow_matched_fields == low_match);
  ut_a(*ilow_matched_bytes == low_bytes);
#endif
  if (!page_rec_is_supremum(next_rec)) {

    *iup_matched_fields = up_match;
    *iup_matched_bytes = up_bytes;
  }

  *ilow_matched_fields = low_match;
  *ilow_matched_bytes = low_bytes;

#ifdef UNIV_SEARCH_PERF_STAT
  page_cur_short_succ++;
#endif
  success = true;
exit_func:
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
  return (success);
}

#endif

#ifdef PAGE_CUR_LE_OR_EXTENDS
/** Checks if the nth field in a record is a character type field which extends
the nth field in tuple, i.e., the field is longer or equal in length and has
common first characters.
@return	true if rec field extends tuple field */
static bool page_cur_rec_field_extends(
    const dtuple_t *tuple, /*!< in: data tuple */
    const rec_t *rec,      /*!< in: record */
    const ulint *offsets,  /*!< in: array returned by rec_get_offsets() */
    ulint n)               /*!< in: compare nth field */
{
  const dtype_t *type;
  const dfield_t *dfield;
  const byte *rec_f;
  ulint rec_f_len;

  ut_ad(rec_offs_validate(rec, nullptr, offsets));
  dfield = dtuple_get_nth_field(tuple, n);

  type = dfield_get_type(dfield);

  rec_f = rec_get_nth_field(rec, offsets, n, &rec_f_len);

  if (type->mtype == DATA_VARCHAR || type->mtype == DATA_CHAR ||
      type->mtype == DATA_FIXBINARY || type->mtype == DATA_BINARY ||
      type->mtype == DATA_BLOB || type->mtype == DATA_VARCLIENT ||
      type->mtype == DATA_CLIENT) {

    if (dfield_get_len(dfield) != UNIV_SQL_NULL && rec_f_len != UNIV_SQL_NULL &&
        rec_f_len >= dfield_get_len(dfield) &&
        !cmp_data_data_slow(type->mtype, type->prtype, dfield_get_data(dfield),
                            dfield_get_len(dfield), rec_f,
                            dfield_get_len(dfield))) {

      return (true);
    }
  }

  return (false);
}
#endif /* PAGE_CUR_LE_OR_EXTENDS */

void page_cur_search_with_match(
    const buf_block_t *block, const dict_index_t *index, const dtuple_t *tuple,
    ulint mode, ulint *iup_matched_fields, ulint *iup_matched_bytes,
    ulint *ilow_matched_fields, ulint *ilow_matched_bytes, page_cur_t *cursor) {
  ulint up;
  ulint low;
  ulint mid;
  page_t *page;
  const page_dir_slot_t *slot;
  const rec_t *up_rec;
  const rec_t *low_rec;
  const rec_t *mid_rec;
  ulint up_matched_fields;
  ulint up_matched_bytes;
  ulint low_matched_fields;
  ulint low_matched_bytes;
  ulint cur_matched_fields;
  ulint cur_matched_bytes;
  int cmp;
#ifdef UNIV_SEARCH_DEBUG
  int dbg_cmp;
  ulint dbg_matched_fields;
  ulint dbg_matched_bytes;
#endif
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(block && tuple && iup_matched_fields && iup_matched_bytes &&
        ilow_matched_fields && ilow_matched_bytes && cursor);
  ut_ad(dtuple_validate(tuple));
#ifdef UNIV_DEBUG
#ifdef PAGE_CUR_DBG
  if (mode != PAGE_CUR_DBG)
#endif /* PAGE_CUR_DBG */
#ifdef PAGE_CUR_LE_OR_EXTENDS
    if (mode != PAGE_CUR_LE_OR_EXTENDS)
#endif /* PAGE_CUR_LE_OR_EXTENDS */
      ut_ad(mode == PAGE_CUR_L || mode == PAGE_CUR_LE || mode == PAGE_CUR_G ||
            mode == PAGE_CUR_GE);
#endif /* UNIV_DEBUG */
  page = buf_block_get_frame(block);

  page_check_dir(page);

#ifdef PAGE_CUR_ADAPT
  if (page_is_leaf(page) && mode == PAGE_CUR_LE &&
      page_header_get_field(page, PAGE_N_DIRECTION) > 3 &&
      page_header_get_ptr(page, PAGE_LAST_INSERT) &&
      page_header_get_field(page, PAGE_DIRECTION) == PAGE_RIGHT) {

    if (page_cur_try_search_shortcut(block, index, tuple, iup_matched_fields,
                                     iup_matched_bytes, ilow_matched_fields,
                                     ilow_matched_bytes, cursor)) {
      return;
    }
  }
#ifdef PAGE_CUR_DBG
  if (mode == PAGE_CUR_DBG) {
    mode = PAGE_CUR_LE;
  }
#endif
#endif

  /* The following flag does not work for non-latin1 char sets because
  cmp_full_field does not tell how many bytes matched */
#ifdef PAGE_CUR_LE_OR_EXTENDS
  ut_a(mode != PAGE_CUR_LE_OR_EXTENDS);
#endif /* PAGE_CUR_LE_OR_EXTENDS */

  /* If mode PAGE_CUR_G is specified, we are trying to position the
  cursor to answer a query of the form "tuple < X", where tuple is
  the input parameter, and X denotes an arbitrary physical record on
  the page. We want to position the cursor on the first X which
  satisfies the condition. */

  up_matched_fields = *iup_matched_fields;
  up_matched_bytes = *iup_matched_bytes;
  low_matched_fields = *ilow_matched_fields;
  low_matched_bytes = *ilow_matched_bytes;

  /* Perform binary search. First the search is done through the page
  directory, after that as a linear search in the list of records
  owned by the upper limit directory slot. */

  low = 0;
  up = page_dir_get_n_slots(page) - 1;

  /* Perform binary search until the lower and upper limit directory
  slots come to the distance 1 of each other */

  while (up - low > 1) {
    mid = (low + up) / 2;
    slot = page_dir_get_nth_slot(page, mid);
    mid_rec = page_dir_slot_get_rec(slot);

    ut_pair_min(&cur_matched_fields, &cur_matched_bytes, low_matched_fields,
                low_matched_bytes, up_matched_fields, up_matched_bytes);

    offsets = rec_get_offsets(mid_rec, index, offsets,
                              dtuple_get_n_fields_cmp(tuple), &heap);

    cmp = cmp_dtuple_rec_with_match(index->cmp_ctx, tuple, mid_rec, offsets,
                                    &cur_matched_fields, &cur_matched_bytes);

    if (likely(cmp > 0)) {
    low_slot_match:
      low = mid;
      low_matched_fields = cur_matched_fields;
      low_matched_bytes = cur_matched_bytes;

    } else if (expect(cmp, -1)) {
#ifdef PAGE_CUR_LE_OR_EXTENDS
      if (mode == PAGE_CUR_LE_OR_EXTENDS &&
          page_cur_rec_field_extends(tuple, mid_rec, offsets,
                                     cur_matched_fields)) {

        goto low_slot_match;
      }
#endif /* PAGE_CUR_LE_OR_EXTENDS */
    up_slot_match:
      up = mid;
      up_matched_fields = cur_matched_fields;
      up_matched_bytes = cur_matched_bytes;

    } else if (mode == PAGE_CUR_G || mode == PAGE_CUR_LE
#ifdef PAGE_CUR_LE_OR_EXTENDS
               || mode == PAGE_CUR_LE_OR_EXTENDS
#endif /* PAGE_CUR_LE_OR_EXTENDS */
    ) {

      goto low_slot_match;
    } else {

      goto up_slot_match;
    }
  }

  slot = page_dir_get_nth_slot(page, low);
  low_rec = page_dir_slot_get_rec(slot);
  slot = page_dir_get_nth_slot(page, up);
  up_rec = page_dir_slot_get_rec(slot);

  /* Perform linear search until the upper and lower records come to
  distance 1 of each other. */

  while (page_rec_get_next_const(low_rec) != up_rec) {

    mid_rec = page_rec_get_next_const(low_rec);

    ut_pair_min(&cur_matched_fields, &cur_matched_bytes, low_matched_fields,
                low_matched_bytes, up_matched_fields, up_matched_bytes);

    offsets = rec_get_offsets(mid_rec, index, offsets,
                              dtuple_get_n_fields_cmp(tuple), &heap);

    cmp = cmp_dtuple_rec_with_match(index->cmp_ctx, tuple, mid_rec, offsets,
                                    &cur_matched_fields, &cur_matched_bytes);

    if (likely(cmp > 0)) {
    low_rec_match:
      low_rec = mid_rec;
      low_matched_fields = cur_matched_fields;
      low_matched_bytes = cur_matched_bytes;

    } else if (expect(cmp, -1)) {
#ifdef PAGE_CUR_LE_OR_EXTENDS
      if (mode == PAGE_CUR_LE_OR_EXTENDS &&
          page_cur_rec_field_extends(tuple, mid_rec, offsets,
                                     cur_matched_fields)) {

        goto low_rec_match;
      }
#endif /* PAGE_CUR_LE_OR_EXTENDS */
    up_rec_match:
      up_rec = mid_rec;
      up_matched_fields = cur_matched_fields;
      up_matched_bytes = cur_matched_bytes;
    } else if (mode == PAGE_CUR_G || mode == PAGE_CUR_LE
#ifdef PAGE_CUR_LE_OR_EXTENDS
               || mode == PAGE_CUR_LE_OR_EXTENDS
#endif /* PAGE_CUR_LE_OR_EXTENDS */
    ) {

      goto low_rec_match;
    } else {

      goto up_rec_match;
    }
  }

#ifdef UNIV_SEARCH_DEBUG

  /* Check that the lower and upper limit records have the
  right alphabetical order compared to tuple. */
  dbg_matched_fields = 0;
  dbg_matched_bytes = 0;

  offsets = rec_get_offsets(low_rec, index, offsets, ULINT_UNDEFINED, &heap);

  dbg_cmp =
      page_cmp_dtuple_rec_with_match(index->cmp_ctx, tuple, low_rec, offsets,
                                     &dbg_matched_fields, &dbg_matched_bytes);

  if (mode == PAGE_CUR_G) {
    ut_a(dbg_cmp >= 0);
  } else if (mode == PAGE_CUR_GE) {
    ut_a(dbg_cmp == 1);
  } else if (mode == PAGE_CUR_L) {
    ut_a(dbg_cmp == 1);
  } else if (mode == PAGE_CUR_LE) {
    ut_a(dbg_cmp >= 0);
  }

  if (!page_rec_is_infimum(low_rec)) {

    ut_a(low_matched_fields == dbg_matched_fields);
    ut_a(low_matched_bytes == dbg_matched_bytes);
  }

  dbg_matched_fields = 0;
  dbg_matched_bytes = 0;

  offsets = rec_get_offsets(up_rec, index, offsets, ULINT_UNDEFINED, &heap);

  dbg_cmp =
      page_cmp_dtuple_rec_with_match(index->cmp_ctx, tuple, up_rec, offsets,
                                     &dbg_matched_fields, &dbg_matched_bytes);

  if (mode == PAGE_CUR_G) {
    ut_a(dbg_cmp == -1);
  } else if (mode == PAGE_CUR_GE) {
    ut_a(dbg_cmp <= 0);
  } else if (mode == PAGE_CUR_L) {
    ut_a(dbg_cmp <= 0);
  } else if (mode == PAGE_CUR_LE) {
    ut_a(dbg_cmp == -1);
  }

  if (!page_rec_is_supremum(up_rec)) {

    ut_a(up_matched_fields == dbg_matched_fields);
    ut_a(up_matched_bytes == dbg_matched_bytes);
  }
#endif
  if (mode <= PAGE_CUR_GE) {
    page_cur_position(up_rec, block, cursor);
  } else {
    page_cur_position(low_rec, block, cursor);
  }

  *iup_matched_fields = up_matched_fields;
  *iup_matched_bytes = up_matched_bytes;
  *ilow_matched_fields = low_matched_fields;
  *ilow_matched_bytes = low_matched_bytes;
  if (likely_null(heap)) {
    mem_heap_free(heap);
  }
}

void page_cur_open_on_rnd_user_rec(buf_block_t *block, page_cur_t *cursor) {
  ulint rnd;
  ulint n_recs = page_get_n_recs(buf_block_get_frame(block));

  page_cur_set_before_first(block, cursor);

  if (unlikely(n_recs == 0)) {

    return;
  }

  rnd = (ulint)(page_cur_lcg_prng() % n_recs);

  do {
    page_cur_move_to_next(cursor);
  } while (rnd--);
}

/** Writes the log record of a record insert on a page. */
static void page_cur_insert_rec_write_log(
    rec_t *insert_rec,   /*!< in: inserted physical record */
    ulint rec_size,      /*!< in: insert_rec size */
    rec_t *cursor_rec,   /*!< in: record the
                         cursor is pointing to */
    dict_index_t *index, /*!< in: record descriptor */
    mtr_t *mtr)          /*!< in: mini-transaction handle */
{
  ulint cur_rec_size;
  ulint extra_size;
  ulint cur_extra_size;
  const byte *ins_ptr;
  byte *log_ptr;
  const byte *log_end;
  ulint i;

  ut_a(rec_size < UNIV_PAGE_SIZE);
  ut_ad(page_align(insert_rec) == page_align(cursor_rec));
  ut_ad(!page_rec_is_comp(insert_rec) == !dict_table_is_comp(index->table));

  {
    mem_heap_t *heap = nullptr;
    ulint cur_offs_[REC_OFFS_NORMAL_SIZE];
    ulint ins_offs_[REC_OFFS_NORMAL_SIZE];

    ulint *cur_offs;
    ulint *ins_offs;

    rec_offs_init(cur_offs_);
    rec_offs_init(ins_offs_);

    cur_offs =
        rec_get_offsets(cursor_rec, index, cur_offs_, ULINT_UNDEFINED, &heap);
    ins_offs =
        rec_get_offsets(insert_rec, index, ins_offs_, ULINT_UNDEFINED, &heap);

    extra_size = rec_offs_extra_size(ins_offs);
    cur_extra_size = rec_offs_extra_size(cur_offs);
    ut_ad(rec_size == rec_offs_size(ins_offs));
    cur_rec_size = rec_offs_size(cur_offs);

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
  }

  ins_ptr = insert_rec - extra_size;

  i = 0;

  if (cur_extra_size == extra_size) {
    ulint min_rec_size = ut_min(cur_rec_size, rec_size);

    const byte *cur_ptr = cursor_rec - cur_extra_size;

    /* Find out the first byte in insert_rec which differs from
    cursor_rec; skip the bytes in the record info */

    do {
      if (*ins_ptr == *cur_ptr) {
        i++;
        ins_ptr++;
        cur_ptr++;
      } else if ((i < extra_size) &&
                 (i >= extra_size - page_rec_get_base_extra_size(insert_rec))) {
        i = extra_size;
        ins_ptr = insert_rec;
        cur_ptr = cursor_rec;
      } else {
        break;
      }
    } while (i < min_rec_size);
  }

  if (mtr_get_log_mode(mtr) != MTR_LOG_SHORT_INSERTS) {

    if (page_rec_is_comp(insert_rec)) {
      log_ptr = mlog_open_and_write_index(mtr, insert_rec, index,
                                          MLOG_COMP_REC_INSERT,
                                          2 + 5 + 1 + 5 + 5 + MLOG_BUF_MARGIN);
      if (unlikely(!log_ptr)) {
        /* Logging in mtr is switched off
        during crash recovery: in that case
        mlog_open returns nullptr */
        return;
      }
    } else {
      log_ptr = mlog_open(mtr, 11 + 2 + 5 + 1 + 5 + 5 + MLOG_BUF_MARGIN);
      if (unlikely(!log_ptr)) {
        /* Logging in mtr is switched off
        during crash recovery: in that case
        mlog_open returns nullptr */
        return;
      }

      log_ptr = mlog_write_initial_log_record_fast(insert_rec, MLOG_REC_INSERT,
                                                   log_ptr, mtr);
    }

    log_end = &log_ptr[2 + 5 + 1 + 5 + 5 + MLOG_BUF_MARGIN];
    /* Write the cursor rec offset as a 2-byte ulint */
    mach_write_to_2(log_ptr, page_offset(cursor_rec));
    log_ptr += 2;
  } else {
    log_ptr = mlog_open(mtr, 5 + 1 + 5 + 5 + MLOG_BUF_MARGIN);
    if (!log_ptr) {
      /* Logging in mtr is switched off during crash
      recovery: in that case mlog_open returns nullptr */
      return;
    }
    log_end = &log_ptr[5 + 1 + 5 + 5 + MLOG_BUF_MARGIN];
  }

  if (page_rec_is_comp(insert_rec)) {
    if (unlikely(rec_get_info_and_status_bits(insert_rec, true) !=
                 rec_get_info_and_status_bits(cursor_rec, true))) {

      goto need_extra_info;
    }
  } else {
    if (unlikely(rec_get_info_and_status_bits(insert_rec, false) !=
                 rec_get_info_and_status_bits(cursor_rec, false))) {

      goto need_extra_info;
    }
  }

  if (extra_size != cur_extra_size || rec_size != cur_rec_size) {
  need_extra_info:
    /* Write the record end segment length
    and the extra info storage flag */
    log_ptr += mach_write_compressed(log_ptr, 2 * (rec_size - i) + 1);

    /* Write the info bits */
    mach_write_to_1(log_ptr, rec_get_info_and_status_bits(
                                 insert_rec, page_rec_is_comp(insert_rec)));
    log_ptr++;

    /* Write the record origin offset */
    log_ptr += mach_write_compressed(log_ptr, extra_size);

    /* Write the mismatch index */
    log_ptr += mach_write_compressed(log_ptr, i);

    ut_a(i < UNIV_PAGE_SIZE);
    ut_a(extra_size < UNIV_PAGE_SIZE);
  } else {
    /* Write the record end segment length
    and the extra info storage flag */
    log_ptr += mach_write_compressed(log_ptr, 2 * (rec_size - i));
  }

  /* Write to the log the inserted index record end segment which
  differs from the cursor record */

  rec_size -= i;

  if (log_ptr + rec_size <= log_end) {
    memcpy(log_ptr, ins_ptr, rec_size);
    mlog_close(mtr, log_ptr + rec_size);
  } else {
    mlog_close(mtr, log_ptr);
    ut_a(rec_size < UNIV_PAGE_SIZE);
    mlog_catenate_string(mtr, ins_ptr, rec_size);
  }
}

byte *page_cur_parse_insert_rec(bool is_short, byte *ptr, byte *end_ptr,
                                buf_block_t *block, dict_index_t *index,
                                mtr_t *mtr) {
  ulint origin_offset;
  ulint mismatch_index;
  page_t *page;
  rec_t *cursor_rec;
  byte buf1[1024];
  byte *buf;
  byte *ptr2 = ptr;
  ulint info_and_status_bits = 0; /* remove warning */
  page_cur_t cursor;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  page = block ? buf_block_get_frame(block) : nullptr;

  if (is_short) {
    cursor_rec = page_rec_get_prev(page_get_supremum_rec(page));
  } else {
    ulint offset;

    /* Read the cursor rec offset as a 2-byte ulint */

    if (unlikely(end_ptr < ptr + 2)) {

      return (nullptr);
    }

    offset = mach_read_from_2(ptr);
    ptr += 2;

    cursor_rec = page + offset;

    if (unlikely(offset >= UNIV_PAGE_SIZE)) {

      recv_sys->found_corrupt_log = true;

      return (nullptr);
    }
  }

  auto end_seg_len = mach_parse_compressed(ptr, end_ptr);

  if (ptr == nullptr) {

    return (nullptr);
  }

  if (unlikely(end_seg_len >= UNIV_PAGE_SIZE << 1)) {
    recv_sys->found_corrupt_log = true;

    return (nullptr);
  }

  if (end_seg_len & 0x1UL) {
    /* Read the info bits */

    if (end_ptr < ptr + 1) {

      return (nullptr);
    }

    info_and_status_bits = mach_read_from_1(ptr);
    ptr++;

    origin_offset = mach_parse_compressed(ptr, end_ptr);

    if (ptr == nullptr) {

      return (nullptr);
    }

    ut_a(origin_offset < UNIV_PAGE_SIZE);

    mismatch_index = mach_parse_compressed(ptr, end_ptr);

    if (ptr == nullptr) {

      return (nullptr);
    }

    ut_a(mismatch_index < UNIV_PAGE_SIZE);
  }

  if (unlikely(end_ptr < ptr + (end_seg_len >> 1))) {

    return (nullptr);
  }

  if (!block) {

    return (ptr + (end_seg_len >> 1));
  }

  ut_ad(!!page_is_comp(page) == dict_table_is_comp(index->table));

  /* Read from the log the inserted index record end segment which
  differs from the cursor record */

  offsets = rec_get_offsets(cursor_rec, index, offsets, ULINT_UNDEFINED, &heap);

  if (!(end_seg_len & 0x1UL)) {
    info_and_status_bits =
        rec_get_info_and_status_bits(cursor_rec, page_is_comp(page));
    origin_offset = rec_offs_extra_size(offsets);
    mismatch_index = rec_offs_size(offsets) - (end_seg_len >> 1);
  }

  end_seg_len >>= 1;

  if (mismatch_index + end_seg_len < sizeof buf1) {
    buf = buf1;
  } else {
    buf = static_cast<byte *>(mem_alloc(mismatch_index + end_seg_len));
  }

  /* Build the inserted record to buf */

  if (unlikely(mismatch_index >= UNIV_PAGE_SIZE)) {
    ib_logger(ib_stream,
              "Is short %lu, info_and_status_bits %lu, offset %lu, "
              "o_offset %lu\n"
              "mismatch index %lu, end_seg_len %lu\n"
              "parsed len %lu\n",
              (ulong)is_short, (ulong)info_and_status_bits,
              (ulong)page_offset(cursor_rec), (ulong)origin_offset,
              (ulong)mismatch_index, (ulong)end_seg_len, (ulong)(ptr - ptr2));

    ib_logger(ib_stream, "Dump of 300 bytes of log:\n");
    ut_print_buf(ib_stream, ptr2, 300);
    ib_logger(ib_stream, "\n");

    buf_page_print(page, 0);

    ut_error;
  }

  memcpy(buf, rec_get_start(cursor_rec, offsets), mismatch_index);
  memcpy(buf + mismatch_index, ptr, end_seg_len);

  if (page_is_comp(page)) {
    rec_set_info_and_status_bits(buf + origin_offset, info_and_status_bits);
  } else {
    rec_set_info_bits_old(buf + origin_offset, info_and_status_bits);
  }

  page_cur_position(cursor_rec, block, &cursor);

  offsets = rec_get_offsets(buf + origin_offset, index, offsets,
                            ULINT_UNDEFINED, &heap);
  if (unlikely(!page_cur_rec_insert(&cursor, buf + origin_offset, index,
                                    offsets, mtr))) {
    /* The redo log record should only have been written
    after the write was successful. */
    ut_error;
  }

  if (buf != buf1) {

    mem_free(buf);
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  return (ptr + end_seg_len);
}

rec_t *page_cur_insert_rec_low(rec_t *current_rec, dict_index_t *index,
                               const rec_t *rec, ulint *offsets, mtr_t *mtr) {
  byte *insert_buf;
  ulint rec_size;
  page_t *page;       /*!< the relevant page */
  rec_t *last_insert; /*!< cursor position at previous
                      insert */
  rec_t *free_rec;    /*!< a free record that was reused,
                      or nullptr */
  rec_t *insert_rec;  /*!< inserted record */
  ulint heap_no;      /*!< heap number of the inserted
                      record */

  ut_ad(rec_offs_validate(rec, index, offsets));

  page = page_align(current_rec);
  ut_ad(dict_table_is_comp(index->table) == (bool)!!page_is_comp(page));

  ut_ad(!page_rec_is_supremum(current_rec));

  /* 1. Get the size of the physical record in the page */
  rec_size = rec_offs_size(offsets);

#ifdef UNIV_DEBUG_VALGRIND
  {
    const void *rec_start = rec - rec_offs_extra_size(offsets);
    ulint extra_size = rec_offs_extra_size(offsets) -
                       (rec_offs_comp(offsets) ? REC_N_NEW_EXTRA_BYTES
                                               : REC_N_OLD_EXTRA_BYTES);

    /* All data bytes of the record must be valid. */
    UNIV_MEM_ASSERT_RW(rec, rec_offs_data_size(offsets));
    /* The variable-length header must be valid. */
    UNIV_MEM_ASSERT_RW(rec_start, extra_size);
  }
#endif /* UNIV_DEBUG_VALGRIND */

  /* 2. Try to find suitable space from page memory management */

  free_rec = page_header_get_ptr(page, PAGE_FREE);
  if (likely_null(free_rec)) {
    /* Try to allocate from the head of the free list. */
    ulint foffsets_[REC_OFFS_NORMAL_SIZE];
    ulint *foffsets = foffsets_;
    mem_heap_t *heap = nullptr;

    rec_offs_init(foffsets_);

    foffsets =
        rec_get_offsets(free_rec, index, foffsets, ULINT_UNDEFINED, &heap);
    if (rec_offs_size(foffsets) < rec_size) {
      if (likely_null(heap)) {
        mem_heap_free(heap);
      }

      goto use_heap;
    }

    insert_buf = free_rec - rec_offs_extra_size(foffsets);

    if (page_is_comp(page)) {
      heap_no = rec_get_heap_no_new(free_rec);
      page_mem_alloc_free(page, rec_get_next_ptr(free_rec, true), rec_size);
    } else {
      heap_no = rec_get_heap_no_old(free_rec);
      page_mem_alloc_free(page, rec_get_next_ptr(free_rec, false), rec_size);
    }

    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
  } else {
  use_heap:
    free_rec = nullptr;
    insert_buf = page_mem_alloc_heap(page, rec_size, &heap_no);

    if (unlikely(insert_buf == nullptr)) {
      return (nullptr);
    }
  }

  /* 3. Create the record */
  insert_rec = rec_copy(insert_buf, rec, offsets);
  rec_offs_make_valid(insert_rec, index, offsets);

  /* 4. Insert the record in the linked list of records */
  ut_ad(current_rec != insert_rec);

  {
    /* next record after current before the insertion */
    rec_t *next_rec = page_rec_get_next(current_rec);
#ifdef UNIV_DEBUG
    if (page_is_comp(page)) {
      ut_ad(rec_get_status(current_rec) <= REC_STATUS_INFIMUM);
      ut_ad(rec_get_status(insert_rec) < REC_STATUS_INFIMUM);
      ut_ad(rec_get_status(next_rec) != REC_STATUS_INFIMUM);
    }
#endif
    page_rec_set_next(insert_rec, next_rec);
    page_rec_set_next(current_rec, insert_rec);
  }

  page_header_set_field(page, PAGE_N_RECS, 1 + page_get_n_recs(page));

  /* 5. Set the n_owned field in the inserted record to zero,
  and set the heap_no field */
  if (page_is_comp(page)) {
    rec_set_n_owned_new(insert_rec, 0);
    rec_set_heap_no_new(insert_rec, heap_no);
  } else {
    rec_set_n_owned_old(insert_rec, 0);
    rec_set_heap_no_old(insert_rec, heap_no);
  }

  UNIV_MEM_ASSERT_RW(rec_get_start(insert_rec, offsets),
                     rec_offs_size(offsets));
  /* 6. Update the last insertion info in page header */

  last_insert = page_header_get_ptr(page, PAGE_LAST_INSERT);
  ut_ad(!last_insert || !page_is_comp(page) ||
        rec_get_node_ptr_flag(last_insert) ==
            rec_get_node_ptr_flag(insert_rec));

  if (unlikely(last_insert == nullptr)) {
    page_header_set_field(page, PAGE_DIRECTION, PAGE_NO_DIRECTION);
    page_header_set_field(page, PAGE_N_DIRECTION, 0);

  } else if ((last_insert == current_rec) &&
             (page_header_get_field(page, PAGE_DIRECTION) != PAGE_LEFT)) {

    page_header_set_field(page, PAGE_DIRECTION, PAGE_RIGHT);
    page_header_set_field(page, PAGE_N_DIRECTION,
                          page_header_get_field(page, PAGE_N_DIRECTION) + 1);

  } else if ((page_rec_get_next(insert_rec) == last_insert) &&
             (page_header_get_field(page, PAGE_DIRECTION) != PAGE_RIGHT)) {

    page_header_set_field(page, PAGE_DIRECTION, PAGE_LEFT);
    page_header_set_field(page, PAGE_N_DIRECTION,
                          page_header_get_field(page, PAGE_N_DIRECTION) + 1);
  } else {
    page_header_set_field(page, PAGE_DIRECTION, PAGE_NO_DIRECTION);
    page_header_set_field(page, PAGE_N_DIRECTION, 0);
  }

  page_header_set_ptr(page, PAGE_LAST_INSERT, insert_rec);

  /* 7. It remains to update the owner record. */
  {
    rec_t *owner_rec = page_rec_find_owner_rec(insert_rec);
    ulint n_owned;
    if (page_is_comp(page)) {
      n_owned = rec_get_n_owned_new(owner_rec);
      rec_set_n_owned_new(owner_rec, n_owned + 1);
    } else {
      n_owned = rec_get_n_owned_old(owner_rec);
      rec_set_n_owned_old(owner_rec, n_owned + 1);
    }

    /* 8. Now we have incremented the n_owned field of the owner
    record. If the number exceeds PAGE_DIR_SLOT_MAX_N_OWNED,
    we have to split the corresponding directory slot in two. */

    if (unlikely(n_owned == PAGE_DIR_SLOT_MAX_N_OWNED)) {
      page_dir_split_slot(page, page_dir_find_owner_slot(owner_rec));
    }
  }

  /* 9. Write log record of the insert */
  if (likely(mtr != nullptr)) {
    page_cur_insert_rec_write_log(insert_rec, rec_size, current_rec, index,
                                  mtr);
  }

  return (insert_rec);
}

/** Writes a log record of copying a record list end to a new created page.
@return 4-byte field where to write the log data length, or nullptr if
logging is disabled */
static byte *page_copy_rec_list_to_created_page_write_log(
    page_t *page,        /*!< in: index page */
    dict_index_t *index, /*!< in: record descriptor */
    mtr_t *mtr)          /*!< in: mtr */
{
  byte *log_ptr;

  ut_ad(!!page_is_comp(page) == dict_table_is_comp(index->table));

  log_ptr = mlog_open_and_write_index(mtr, page, index,
                                      page_is_comp(page)
                                          ? MLOG_COMP_LIST_END_COPY_CREATED
                                          : MLOG_LIST_END_COPY_CREATED,
                                      4);
  if (likely(log_ptr != nullptr)) {
    mlog_close(mtr, log_ptr + 4);
  }

  return (log_ptr);
}

byte *page_parse_copy_rec_list_to_created_page(byte *ptr, byte *end_ptr,
                                               buf_block_t *block,
                                               dict_index_t *index,
                                               mtr_t *mtr) {
  byte *rec_end;
  ulint log_data_len;
  page_t *page;

  if (ptr + 4 > end_ptr) {

    return (nullptr);
  }

  log_data_len = mach_read_from_4(ptr);
  ptr += 4;

  rec_end = ptr + log_data_len;

  if (rec_end > end_ptr) {

    return (nullptr);
  }

  if (!block) {

    return (rec_end);
  }

  while (ptr < rec_end) {
    ptr = page_cur_parse_insert_rec(true, ptr, end_ptr, block, index, mtr);
  }

  ut_a(ptr == rec_end);

  page = buf_block_get_frame(block);

  page_header_set_ptr(page, PAGE_LAST_INSERT, nullptr);
  page_header_set_field(page, PAGE_DIRECTION, PAGE_NO_DIRECTION);
  page_header_set_field(page, PAGE_N_DIRECTION, 0);

  return (rec_end);
}

/** Copies records from page to a newly created page, from a given record
onward, including that record. Infimum and supremum records are not copied. */

void page_copy_rec_list_end_to_created_page(
    page_t *new_page,    /*!< in/out: index page to copy to */
    rec_t *rec,          /*!< in: first record to copy */
    dict_index_t *index, /*!< in: record descriptor */
    mtr_t *mtr)          /*!< in: mtr */
{
  page_dir_slot_t *slot = 0; /* remove warning */
  byte *heap_top;
  rec_t *insert_rec = 0; /* remove warning */
  rec_t *prev_rec;
  ulint count;
  ulint n_recs;
  ulint slot_index;
  ulint rec_size;
  ulint log_mode;
  byte *log_ptr;
  ulint log_data_len;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(page_dir_get_n_heap(new_page) == PAGE_HEAP_NO_USER_LOW);
  ut_ad(page_align(rec) != new_page);
  ut_ad(page_rec_is_comp(rec) == page_is_comp(new_page));

  if (page_rec_is_infimum(rec)) {

    rec = page_rec_get_next(rec);
  }

  if (page_rec_is_supremum(rec)) {

    return;
  }

#ifdef UNIV_DEBUG
  /* To pass the debug tests we have to set these dummy values
  in the debug version */
  page_dir_set_n_slots(new_page, UNIV_PAGE_SIZE / 2);
  page_header_set_ptr(new_page, PAGE_HEAP_TOP, new_page + UNIV_PAGE_SIZE - 1);
#endif /* UNIV_DEBUG */

  log_ptr = page_copy_rec_list_to_created_page_write_log(new_page, index, mtr);

  log_data_len = dyn_array_get_data_size(&(mtr->log));

  /* Individual inserts are logged in a shorter form */

  log_mode = mtr_set_log_mode(mtr, MTR_LOG_SHORT_INSERTS);

  prev_rec = page_get_infimum_rec(new_page);
  if (page_is_comp(new_page)) {
    heap_top = new_page + PAGE_NEW_SUPREMUM_END;
  } else {
    heap_top = new_page + PAGE_OLD_SUPREMUM_END;
  }
  count = 0;
  slot_index = 0;
  n_recs = 0;

  do {
    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);
    insert_rec = rec_copy(heap_top, rec, offsets);

    if (page_is_comp(new_page)) {
      rec_set_next_offs_new(prev_rec, page_offset(insert_rec));

      rec_set_n_owned_new(insert_rec, 0);
      rec_set_heap_no_new(insert_rec, PAGE_HEAP_NO_USER_LOW + n_recs);
    } else {
      rec_set_next_offs_old(prev_rec, page_offset(insert_rec));

      rec_set_n_owned_old(insert_rec, 0);
      rec_set_heap_no_old(insert_rec, PAGE_HEAP_NO_USER_LOW + n_recs);
    }

    count++;
    n_recs++;

    if (unlikely(count == (PAGE_DIR_SLOT_MAX_N_OWNED + 1) / 2)) {

      slot_index++;

      slot = page_dir_get_nth_slot(new_page, slot_index);

      page_dir_slot_set_rec(slot, insert_rec);
      page_dir_slot_set_n_owned(slot, count);

      count = 0;
    }

    rec_size = rec_offs_size(offsets);

    ut_ad(heap_top < new_page + UNIV_PAGE_SIZE);

    heap_top += rec_size;

    page_cur_insert_rec_write_log(insert_rec, rec_size, prev_rec, index, mtr);
    prev_rec = insert_rec;
    rec = page_rec_get_next(rec);
  } while (!page_rec_is_supremum(rec));

  if ((slot_index > 0) && (count + 1 + (PAGE_DIR_SLOT_MAX_N_OWNED + 1) / 2 <=
                           PAGE_DIR_SLOT_MAX_N_OWNED)) {
    /* We can merge the two last dir slots. This operation is
    here to make this function imitate exactly the equivalent
    task made using page_cur_insert_rec, which we use in database
    recovery to reproduce the task performed by this function.
    To be able to check the correctness of recovery, it is good
    that it imitates exactly. */

    count += (PAGE_DIR_SLOT_MAX_N_OWNED + 1) / 2;

    page_dir_slot_set_n_owned(slot, 0);

    slot_index--;
  }

  if (likely_null(heap)) {
    mem_heap_free(heap);
  }

  log_data_len = dyn_array_get_data_size(&(mtr->log)) - log_data_len;

  ut_a(log_data_len < 100 * UNIV_PAGE_SIZE);

  if (likely(log_ptr != nullptr)) {
    mach_write_to_4(log_ptr, log_data_len);
  }

  if (page_is_comp(new_page)) {
    rec_set_next_offs_new(insert_rec, PAGE_NEW_SUPREMUM);
  } else {
    rec_set_next_offs_old(insert_rec, PAGE_OLD_SUPREMUM);
  }

  slot = page_dir_get_nth_slot(new_page, 1 + slot_index);

  page_dir_slot_set_rec(slot, page_get_supremum_rec(new_page));
  page_dir_slot_set_n_owned(slot, count + 1);

  page_dir_set_n_slots(new_page, 2 + slot_index);
  page_header_set_ptr(new_page, PAGE_HEAP_TOP, heap_top);
  page_dir_set_n_heap(new_page, PAGE_HEAP_NO_USER_LOW + n_recs);
  page_header_set_field(new_page, PAGE_N_RECS, n_recs);

  page_header_set_ptr(new_page, PAGE_LAST_INSERT, nullptr);
  page_header_set_field(new_page, PAGE_N_DIRECTION, 0);

  /* Restore the log mode */

  mtr_set_log_mode(mtr, log_mode);
}

/** Writes log record of a record delete on a page. */
static void
page_cur_delete_rec_write_log(rec_t *rec, /*!< in: record to be deleted */
                              dict_index_t *index, /*!< in: record descriptor */
                              mtr_t *mtr) /*!< in: mini-transaction handle */
{
  byte *log_ptr;

  ut_ad(!!page_rec_is_comp(rec) == dict_table_is_comp(index->table));

  log_ptr = mlog_open_and_write_index(
      mtr, rec, index,
      page_rec_is_comp(rec) ? MLOG_COMP_REC_DELETE : MLOG_REC_DELETE, 2);

  if (!log_ptr) {
    /* Logging in mtr is switched off during crash recovery:
    in that case mlog_open returns nullptr */
    return;
  }

  /* Write the cursor rec offset as a 2-byte ulint */
  mach_write_to_2(log_ptr, page_offset(rec));

  mlog_close(mtr, log_ptr + 2);
}

byte *page_cur_parse_delete_rec(byte *ptr, byte *end_ptr, buf_block_t *block,
                                dict_index_t *index, mtr_t *mtr) {
  ulint offset;
  page_cur_t cursor;

  if (end_ptr < ptr + 2) {

    return (nullptr);
  }

  /* Read the cursor rec offset as a 2-byte ulint */
  offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(offset <= UNIV_PAGE_SIZE);

  if (block) {
    page_t *page = buf_block_get_frame(block);
    mem_heap_t *heap = nullptr;
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    rec_t *rec = page + offset;
    rec_offs_init(offsets_);

    page_cur_position(rec, block, &cursor);

    page_cur_delete_rec(
        &cursor, index,
        rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED, &heap), mtr);
    if (likely_null(heap)) {
      mem_heap_free(heap);
    }
  }

  return (ptr);
}

void page_cur_delete_rec(page_cur_t *cursor, dict_index_t *index,
                         const ulint *offsets, mtr_t *mtr) {
  page_dir_slot_t *cur_dir_slot;
  page_dir_slot_t *prev_slot;
  page_t *page;
  rec_t *current_rec;
  rec_t *prev_rec = nullptr;
  rec_t *next_rec;
  ulint cur_slot_no;
  ulint cur_n_owned;
  rec_t *rec;

  ut_ad(cursor && mtr);

  page = page_cur_get_page(cursor);

  current_rec = cursor->rec;
  ut_ad(rec_offs_validate(current_rec, index, offsets));
  ut_ad(!!page_is_comp(page) == dict_table_is_comp(index->table));

  /* The record must not be the supremum or infimum record. */
  ut_ad(page_rec_is_user_rec(current_rec));

  /* Save to local variables some data associated with current_rec */
  cur_slot_no = page_dir_find_owner_slot(current_rec);
  cur_dir_slot = page_dir_get_nth_slot(page, cur_slot_no);
  cur_n_owned = page_dir_slot_get_n_owned(cur_dir_slot);

  /* 0. Write the log record */
  page_cur_delete_rec_write_log(current_rec, index, mtr);

  /* 1. Reset the last insert info in the page header and increment
  the modify clock for the frame */

  page_header_set_ptr(page, PAGE_LAST_INSERT, nullptr);

  /* The page gets invalid for optimistic searches: increment the
  frame modify clock */

  buf_block_modify_clock_inc(page_cur_get_block(cursor));

  /* 2. Find the next and the previous record. Note that the cursor is
  left at the next record. */

  ut_ad(cur_slot_no > 0);
  prev_slot = page_dir_get_nth_slot(page, cur_slot_no - 1);

  rec = (rec_t *)page_dir_slot_get_rec(prev_slot);

  /* rec now points to the record of the previous directory slot. Look
  for the immediate predecessor of current_rec in a loop. */

  while (current_rec != rec) {
    prev_rec = rec;
    rec = page_rec_get_next(rec);
  }

  page_cur_move_to_next(cursor);
  next_rec = cursor->rec;

  /* 3. Remove the record from the linked list of records */

  page_rec_set_next(prev_rec, next_rec);

  /* 4. If the deleted record is pointed to by a dir slot, update the
  record pointer in slot. In the following if-clause we assume that
  prev_rec is owned by the same slot, i.e., PAGE_DIR_SLOT_MIN_N_OWNED
  >= 2. */

  static_assert(PAGE_DIR_SLOT_MIN_N_OWNED >= 2,
                "error PAGE_DIR_SLOT_MIN_N_OWNED < 2");

  ut_ad(cur_n_owned > 1);

  if (current_rec == page_dir_slot_get_rec(cur_dir_slot)) {
    page_dir_slot_set_rec(cur_dir_slot, prev_rec);
  }

  /* 5. Update the number of owned records of the slot */

  page_dir_slot_set_n_owned(cur_dir_slot, cur_n_owned - 1);

  /* 6. Free the memory occupied by the record */
  page_mem_free(page, current_rec, index, offsets);

  /* 7. Now we have decremented the number of owned records of the slot.
  If the number drops below PAGE_DIR_SLOT_MIN_N_OWNED, we balance the
  slots. */

  if (unlikely(cur_n_owned <= PAGE_DIR_SLOT_MIN_N_OWNED)) {
    page_dir_balance_slot(page, cur_slot_no);
  }
}
