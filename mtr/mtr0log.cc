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

/** @file mtr/mtr0log.c
Mini-transaction log routines

Created 12/7/1995 Heikki Tuuri
*******************************************************/

#include "mtr0log.h"

#include "buf0buf.h"
#include "dict0boot.h"
#include "dict0dict.h"
#include "log0recv.h"
#include "page0page.h"
#include "trx0sys.h"

void mlog_catenate_string(mtr_t *mtr, const byte *str, ulint len) {
  if (mtr->get_log_mode() == MTR_LOG_NONE) {

    return;
  }

  auto mlog = &mtr->m_log;

  dyn_push_string(mlog, str, len);
}

void mlog_write_initial_log_record(const byte *ptr, mlog_type_t type, mtr_t *mtr) {
  ut_ad(type <= MLOG_BIGGEST_TYPE);
  ut_ad(type > MLOG_8BYTES);

  auto log_ptr = mlog_open(mtr, 11);

  /* If no logging is requested, we may return now */
  if (log_ptr == nullptr) {

    return;
  }

  log_ptr = mlog_write_initial_log_record_fast(ptr, type, log_ptr, mtr);

  mlog_close(mtr, log_ptr);
}

byte *mlog_parse_initial_log_record(byte *ptr, byte *end_ptr, mlog_type_t *type, ulint *space, ulint *page_no) {
  if (end_ptr < ptr + 1) {

    return (nullptr);
  }

  *type = static_cast<mlog_type_t>((ulint)*ptr & ~MLOG_SINGLE_REC_FLAG);
  ut_ad(*type <= MLOG_BIGGEST_TYPE);

  ++ptr;

  if (end_ptr < ptr + 2) {

    return (nullptr);
  }

  *space = static_cast<space_id_t>(mach_parse_compressed(ptr, end_ptr));

  if (ptr == nullptr) {

    return (nullptr);
  }

  *page_no = static_cast<page_no_t>(mach_parse_compressed(ptr, end_ptr));

  return ptr;
}

byte *mlog_parse_nbytes(mlog_type_t type, byte *ptr, byte *end_ptr, byte *page) {
  ulint val;
  uint64_t dval;

  ut_a(type <= MLOG_8BYTES);

  if (end_ptr < ptr + 2) {

    return (nullptr);
  }

  auto offset = mach_read_from_2(ptr);

  ptr += 2;

  if (offset >= UNIV_PAGE_SIZE) {
    recv_sys->m_found_corrupt_log = true;

    return nullptr;
  }

  if (type == MLOG_8BYTES) {
    ptr = mach_uint64_parse_compressed(ptr, end_ptr, &dval);

    if (ptr == nullptr) {

      return nullptr;
    }

    if (page != nullptr) {
      mach_write_to_8(page + offset, dval);
    }

    return ptr;
  }

  val = mach_parse_compressed(ptr, end_ptr);

  if (ptr == nullptr) {

    return nullptr;
  }

  switch (type) {
    case MLOG_1BYTE:
      if (unlikely(val > 0xFFUL)) {
        goto corrupt;
      }
      if (page != nullptr) {
        mach_write_to_1(page + offset, val);
      }
      break;
    case MLOG_2BYTES:
      if (unlikely(val > 0xFFFFUL)) {
        goto corrupt;
      }
      if (page != nullptr) {
        mach_write_to_2(page + offset, val);
      }
      break;
    case MLOG_4BYTES:
      if (page != nullptr) {
        mach_write_to_4(page + offset, val);
      }
      break;
    default:
    corrupt:
      recv_sys->m_found_corrupt_log = true;
      ptr = nullptr;
  }

  return ptr;
}

void mlog_write_ulint(byte *ptr, ulint val, mlog_type_t type, mtr_t *mtr) {
  switch (type) {
    case MLOG_1BYTE:
      mach_write_to_1(ptr, val);
      break;
    case MLOG_2BYTES:
      mach_write_to_2(ptr, val);
      break;
    case MLOG_4BYTES:
      mach_write_to_4(ptr, val);
      break;
    default:
      ut_error;
  }

  auto log_ptr = mlog_open(mtr, 11 + 2 + 5);

  /* If no logging is requested, we may return now */
  if (log_ptr == nullptr) {

    return;
  }

  log_ptr = mlog_write_initial_log_record_fast(ptr, type, log_ptr, mtr);

  mach_write_to_2(log_ptr, page_offset(ptr));
  log_ptr += 2;

  log_ptr += mach_write_compressed(log_ptr, val);

  mlog_close(mtr, log_ptr);
}

void mlog_write_uint64(byte *ptr, uint64_t val, mtr_t *mtr) {
  mach_write_to_8(ptr, val);

  auto log_ptr = mlog_open(mtr, 11 + 2 + 9);

  /* If no logging is requested, we may return now */
  if (log_ptr == nullptr) {

    return;
  }

  log_ptr = mlog_write_initial_log_record_fast(ptr, MLOG_8BYTES, log_ptr, mtr);

  mach_write_to_2(log_ptr, page_offset(ptr));
  log_ptr += 2;

  log_ptr += mach_uint64_write_compressed(log_ptr, val);

  mlog_close(mtr, log_ptr);
}

void mlog_write_string(byte *ptr, const byte *str, ulint len, mtr_t *mtr) {
  ut_a(len < UNIV_PAGE_SIZE);

  memcpy(ptr, str, len);

  mlog_log_string(ptr, len, mtr);
}

void mlog_log_string(byte *ptr, ulint len, mtr_t *mtr) {
  ut_ad(len <= UNIV_PAGE_SIZE);

  auto log_ptr = mlog_open(mtr, 30);

  /* If no logging is requested, we may return now */
  if (log_ptr == nullptr) {

    return;
  }

  log_ptr = mlog_write_initial_log_record_fast(ptr, MLOG_WRITE_STRING, log_ptr, mtr);
  mach_write_to_2(log_ptr, page_offset(ptr));
  log_ptr += 2;

  mach_write_to_2(log_ptr, len);
  log_ptr += 2;

  mlog_close(mtr, log_ptr);

  mlog_catenate_string(mtr, ptr, len);
}

byte *mlog_parse_string(byte *ptr, byte *end_ptr, byte *page) {
  ut_a(page == nullptr || srv_fil->page_get_type(page) != FIL_PAGE_TYPE_INDEX);

  if (end_ptr < ptr + 4) {

    return nullptr;
  }

  ulint offset = mach_read_from_2(ptr);
  ptr += 2;

  ulint len = mach_read_from_2(ptr);
  ptr += 2;

  if (offset >= UNIV_PAGE_SIZE || len + offset > UNIV_PAGE_SIZE) {
    recv_sys->m_found_corrupt_log = true;

    return nullptr;
  }

  if (end_ptr < ptr + len) {

    return nullptr;
  }

  if (page != nullptr) {
    memcpy(page + offset, ptr, len);
  }

  return ptr + len;
}

byte *mlog_open_and_write_index(mtr_t *mtr, const byte *rec, dict_index_t *index, mlog_type_t type, ulint size) {
  auto log_ptr = mlog_open(mtr, 11 + size);

  if (log_ptr == nullptr) {
    /* logging is disabled */
    return nullptr;
  }

  log_ptr = mlog_write_initial_log_record_fast(rec, type, log_ptr, mtr);
  auto log_end = log_ptr + 11 + size;

  if (size == 0) {
    mlog_close(mtr, log_ptr);
    log_ptr = nullptr;
  } else if (log_ptr + size > log_end) {
    mlog_close(mtr, log_ptr);
    log_ptr = mlog_open(mtr, size);
  }
  return log_ptr;
}

byte *mlog_parse_index(byte *ptr, const byte *end_ptr, dict_index_t **index) {

  auto table = dict_mem_table_create("LOG_DUMMY", DICT_HDR_SPACE, 1, 0);
  auto ind = dict_mem_index_create("LOG_DUMMY", "LOG_DUMMY", DICT_HDR_SPACE, 0, 1);

  ind->n_uniq = 1;
  ind->table = table;

  /* Avoid ut_ad(index->cached) in dict_index_get_n_unique_in_tree */
  *index = ind;
  ind->cached = true;

  return ptr;
}

byte *mlog_write_initial_log_record_fast(const byte *ptr, mlog_type_t type, byte *log_ptr, mtr_t *mtr) {
  ut_ad(mtr->memo_contains_page(ptr, MTR_MEMO_PAGE_X_FIX));
  ut_ad(type <= MLOG_BIGGEST_TYPE);

  auto page = static_cast<const byte *>(ut_align_down(ptr, UNIV_PAGE_SIZE));
  auto space = mach_read_from_4(page + FIL_PAGE_SPACE_ID);
  auto offset = mach_read_from_4(page + FIL_PAGE_OFFSET);

  /* check whether the page is in the doublewrite buffer;
  the doublewrite buffer is located in pages
  FSP_EXTENT_SIZE, ..., 3 * FSP_EXTENT_SIZE - 1 in the
  system tablespace */
  if (space == SYS_TABLESPACE && offset >= FSP_EXTENT_SIZE && offset < 3 * FSP_EXTENT_SIZE) {
    if (trx_doublewrite_buf_is_being_created) {
      /* Do nothing: we only come to this branch in an
      InnoDB database creation. We do not redo log
      anything for the doublewrite buffer pages. */
      return log_ptr;
    } else {
      log_err(std::format(
        "Trying to redo log a record of type {} on page {} of space {} in the"
        " doublewrite buffer, continuing anyway. Please create an issue on the"
        " GitHub project page.",
        ulong(type),
        offset,
        space
      ));
    }
  }

  mach_write_to_1(log_ptr, type);

  ++log_ptr;
  log_ptr += mach_write_compressed(log_ptr, space);
  log_ptr += mach_write_compressed(log_ptr, offset);

  ++mtr->m_n_log_recs;

#ifdef UNIV_DEBUG
  /* We now assume that all x-latched pages have been modified! */
  auto block = srv_buf_pool->block_align(ptr);

  if (!mtr->memo_contains(block, MTR_MEMO_MODIFY)) {

    mtr->memo_push(block, MTR_MEMO_MODIFY);
  }
#endif /* UNIV_DEBUG */

  return log_ptr;
}
