/****************************************************************************
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

/** @file log/recv_parse.cc
Recovery log parsing

*******************************************************/

#include "btr0btr.h"
#include "btr0cur.h"
#include "log0recv.h"
#include "mtr0log.h"
#include "page0cur.h"
#include "srv0srv.h"

#include "trx0undo.h"
#include "trx0undo_rec.h"

static ulint recv_previous_parsed_rec_type;
static ulint recv_previous_parsed_rec_offset;
static ulint recv_previous_parsed_rec_is_multi;

byte *recv_parse_or_apply_log_rec_body(mlog_type_t type, byte *ptr, byte *end_ptr, Buf_block *block, mtr_t *mtr) noexcept;

static ulint recv_parse_log_rec(
  byte *ptr, byte *end_ptr, mlog_type_t *type, space_id_t *space, page_no_t *page_no, byte **body
) noexcept {
  *body = nullptr;

  if (ptr == end_ptr) {
    return 0;
  }

  if (*ptr == MLOG_MULTI_REC_END) {
    *type = static_cast<mlog_type_t>(*ptr);
    return 1;
  }

  if (*ptr == MLOG_DUMMY_RECORD) {
    *type = static_cast<mlog_type_t>(*ptr);
    *space = ULINT32_UNDEFINED - 1;
    return 1;
  }

  auto new_ptr = mlog_parse_initial_log_record(ptr, end_ptr, type, space, page_no);

  *body = new_ptr;

  if (unlikely(!new_ptr)) {
    return 0;
  }

  new_ptr = recv_parse_or_apply_log_rec_body(*type, new_ptr, end_ptr, nullptr, nullptr);

  if (unlikely(new_ptr == nullptr)) {
    return 0;
  }

  if (*page_no > recv_max_parsed_page_no) {
    recv_max_parsed_page_no = *page_no;
  }

  return new_ptr - ptr;
}

byte *recv_parse_or_apply_log_rec_body(mlog_type_t type, byte *ptr, byte *end_ptr, Buf_block *block, mtr_t *mtr) noexcept {
  page_t *page{};
  Index *index{};
  ut_d(Fil_page_type page_type{FIL_PAGE_TYPE_ALLOCATED});

  ut_ad(!block == !mtr);

  if (block != nullptr) {
    page = block->m_frame;
    ut_d(page_type = srv_fil->page_get_type(page));
  }

  switch (type) {
    case MLOG_1BYTE:
    case MLOG_2BYTES:
    case MLOG_4BYTES:
    case MLOG_8BYTES:
#ifdef UNIV_DEBUG
      if (page != nullptr && page_type == FIL_PAGE_TYPE_ALLOCATED && end_ptr >= ptr + 2) {
        ulint offs = mach_read_from_2(ptr);

        switch (type) {
          default:
            ut_error;
          case MLOG_2BYTES:
            ut_ad(
              offs == FIL_PAGE_TYPE || offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_OFFSET ||
              offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_OFFSET
            );
            break;
          case MLOG_4BYTES:
            ut_ad(
              offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_PAGE_NO ||
              offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_SPACE ||
              offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_PAGE_NO || offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_SPACE
            );
            break;
        }
      }
#endif
      ptr = mlog_parse_nbytes(type, ptr, end_ptr, page);
      break;
    case MLOG_REC_INSERT:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = Page_cursor::parse_insert_rec(false, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_REC_CLUST_DELETE_MARK:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = Btree_cursor::parse_del_mark_set_clust_rec(ptr, end_ptr, page, index);
      }
    case MLOG_REC_SEC_DELETE_MARK:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);
      ptr = Btree_cursor::parse_del_mark_set_sec_rec(ptr, end_ptr, page);
      break;
    case MLOG_REC_UPDATE_IN_PLACE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = Btree_cursor::parse_update_in_place(ptr, end_ptr, page, index);
      }
      break;
    case MLOG_LIST_END_DELETE:
    case MLOG_LIST_START_DELETE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = page_parse_delete_rec_list(type, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_LIST_END_COPY_CREATED:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = Page_cursor::parse_copy_rec_list_to_created_page(ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_PAGE_REORGANIZE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = srv_btree_sys->parse_page_reorganize(ptr, end_ptr, index, block, mtr);
      }
      break;
    case MLOG_PAGE_CREATE:
      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = page_parse_create(ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_UNDO_INSERT:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = Trx_undo_record::parse_add_undo_rec(ptr, end_ptr, page);
      break;
    case MLOG_UNDO_ERASE_END:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = Trx_undo_record::parse_erase_page_end(ptr, end_ptr, page, mtr);
      break;
    case MLOG_UNDO_INIT:
      ptr = Undo::parse_page_init(ptr, end_ptr, page, mtr);
      break;
    case MLOG_UNDO_HDR_DISCARD:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = Undo::parse_discard_latest(ptr, IF_DEBUG(end_ptr, ) page, mtr);
      break;
    case MLOG_UNDO_HDR_CREATE:
    case MLOG_UNDO_HDR_REUSE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = Undo::parse_page_header(type, ptr, end_ptr, page, mtr);
      break;
    case MLOG_REC_MIN_MARK:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);
      ptr = srv_btree_sys->parse_set_min_rec_mark(ptr, end_ptr, page, mtr);
      break;
    case MLOG_REC_DELETE:
      ut_ad(!page || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = Page_cursor::parse_delete_rec(ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_INIT_FILE_PAGE:
      ptr = srv_fsp->parse_init_file_page(ptr, end_ptr, block);
      break;
    case MLOG_WRITE_STRING:
      ut_ad(!page || page_type != FIL_PAGE_TYPE_ALLOCATED);
      ptr = mlog_parse_string(ptr, end_ptr, page);
      break;
    case MLOG_FILE_CREATE:
    case MLOG_FILE_RENAME:
    case MLOG_FILE_DELETE:
      ptr = srv_fil->op_log_parse_or_replay(ptr, end_ptr, type, 0, 0);
      break;
    default:
      ptr = nullptr;
      recv_sys->m_found_corrupt_log = true;
  }

  if (index != nullptr) {
    auto table = index->m_table;
    Index::destroy(index, Current_location());
    Table::destroy(table, Current_location());
  }

  return ptr;
}

static lsn_t recv_calc_lsn_on_data_add(lsn_t lsn, uint64_t len) noexcept {
  auto frag_len = (ulint(lsn) % IB_FILE_BLOCK_SIZE) - LOG_BLOCK_HDR_SIZE;

  ut_ad(frag_len < IB_FILE_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE);
  auto lsn_len = ulint(len);

  lsn_len += (lsn_len + frag_len) / (IB_FILE_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE) *
             (LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE);

  return lsn + lsn_len;
}

static void recv_report_corrupt_log(byte *ptr, byte type, space_id_t space, page_no_t page_no) noexcept {
  log_err(std::format(
    "############### CORRUPT LOG RECORD FOUND\n"
    "Log record type {}, space id {}, page number {}\n"
    "Log parsing proceeded successfully up to {}\n"
    "Previous log record type {}, is multi {}\n"
    "Recv offset {}, prev {}",
    (ulong)type,
    space,
    page_no,
    recv_sys->m_recovered_lsn,
    (ulong)recv_previous_parsed_rec_type,
    recv_previous_parsed_rec_is_multi,
    ptr - recv_sys->m_buf,
    recv_previous_parsed_rec_offset
  ));

  if (ulint(ptr - recv_sys->m_buf + 100) > recv_previous_parsed_rec_offset &&
      ulint(ptr - recv_sys->m_buf + 100 - recv_previous_parsed_rec_offset) < 200000) {
    log_err(
      "Hex dump of corrupt log starting 100 bytes before the start"
      " of the previous log rec, and ending 100 bytes after the start"
      " of the corrupt rec:"
    );

    log_warn_buf(
      recv_sys->m_buf + recv_previous_parsed_rec_offset - 100, ptr - recv_sys->m_buf + 200 - recv_previous_parsed_rec_offset
    );
  }

  if (!srv_config.m_force_recovery) {
    log_fatal("Set innodb_force_recovery to ignore this error.");
  }

  log_warn(
    "The log file may have been corrupt and it s possible that the log scan did not proceed"
    " far enough in recovery! Please run CHECK TABLE on your InnoDB tables to check that"
    " they are ok! If the engine crashes after this recovery, check the InnoDB website"
    " for details about forcing recovery."
  );
}

bool recv_parse_log_recs(bool store_to_hash) noexcept {
  ulint len;
  lsn_t old_lsn;
  ulint total_len;
  mlog_type_t type;
  byte *body;
  space_id_t space;
  page_no_t page_no;
  lsn_t new_recovered_lsn;

  ut_ad(mutex_own(&log_sys->m_mutex));
  ut_ad(recv_sys->m_parse_start_lsn != 0);

  for (;;) {
    auto ptr = recv_sys->m_buf + recv_sys->m_recovered_offset;
    auto end_ptr = recv_sys->m_buf + recv_sys->m_len;

    if (ptr == end_ptr) {
      return false;
    }

    auto single_rec = ulint(*ptr & MLOG_SINGLE_REC_FLAG);

    if (single_rec || *ptr == MLOG_DUMMY_RECORD) {
      old_lsn = recv_sys->m_recovered_lsn;

      len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);

      if (len == 0 || recv_sys->m_found_corrupt_log) {
        if (recv_sys->m_found_corrupt_log) {
          recv_report_corrupt_log(ptr, type, space, page_no);
        }
        return false;
      }

      new_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

      if (new_recovered_lsn > recv_sys->m_scanned_lsn) {
        return false;
      }

      recv_previous_parsed_rec_type = (ulint)type;
      recv_previous_parsed_rec_offset = recv_sys->m_recovered_offset;
      recv_previous_parsed_rec_is_multi = 0;

      recv_sys->m_recovered_offset += len;
      recv_sys->m_recovered_lsn = new_recovered_lsn;

      if (type == MLOG_DUMMY_RECORD) {
        // Do nothing
      } else if (!store_to_hash) {
        // Debug checking mode
      } else if (type == MLOG_FILE_CREATE || type == MLOG_FILE_RENAME || type == MLOG_FILE_DELETE) {
        ut_a(space != SYS_TABLESPACE);
        // File operations not replayed in normal crash recovery
      } else {
        recv_sys->add_log_record(type, space, page_no, body, ptr + len, old_lsn, recv_sys->m_recovered_lsn);
      }
    } else {
      // Multi-record transaction
      total_len = 0;

      for (;;) {
        auto len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);

        if (len == 0 || recv_sys->m_found_corrupt_log) {
          if (recv_sys->m_found_corrupt_log) {
            recv_report_corrupt_log(ptr, type, space, page_no);
          }
          return false;
        }

        recv_previous_parsed_rec_type = (ulint)type;
        recv_previous_parsed_rec_offset = recv_sys->m_recovered_offset + total_len;
        recv_previous_parsed_rec_is_multi = 1;

        total_len += len;
        ptr += len;

        if (type == MLOG_MULTI_REC_END) {
          break;
        }
      }

      new_recovered_lsn = recv_calc_lsn_on_data_add(recv_sys->m_recovered_lsn, total_len);

      if (new_recovered_lsn > recv_sys->m_scanned_lsn) {
        return false;
      }

      // Add all records to hash table
      ptr = recv_sys->m_buf + recv_sys->m_recovered_offset;

      for (;;) {
        old_lsn = recv_sys->m_recovered_lsn;

        len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);

        if (recv_sys->m_found_corrupt_log) {
          recv_report_corrupt_log(ptr, type, space, page_no);
        }

        ut_a(len != 0);
        ut_a(((ulint)*ptr & MLOG_SINGLE_REC_FLAG) == 0);

        recv_sys->m_recovered_offset += len;
        recv_sys->m_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

        if (type == MLOG_MULTI_REC_END) {
          break;
        }

        if (store_to_hash) {
          recv_sys->add_log_record(type, space, page_no, body, ptr + len, old_lsn, new_recovered_lsn);
        }

        ptr += len;
      }
    }
  }
}
