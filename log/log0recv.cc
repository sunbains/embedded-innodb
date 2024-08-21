/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.
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

/** @file log/log0recv.c
Recovery

Created 9/20/1997 Heikki Tuuri
*******************************************************/

#include "log0recv.h"

#include "btr0btr.h"
#include "btr0cur.h"
#include "buf0buf.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "ddl0ddl.h"
#include "fil0fil.h"
#include "mem0mem.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "page0cur.h"
#include "srv0srv.h"
#include "srv0start.h"
#include "sync0sync.h"
#include "trx0rec.h"
#include "trx0roll.h"
#include "trx0undo.h"

/** Log records are stored in the hash table in chunks at most of this size;
this must be less than UNIV_PAGE_SIZE as it is stored in the buffer pool */
constexpr ulint RECV_DATA_BLOCK_SIZE = MEM_MAX_ALLOC_IN_BUF - sizeof(Log_record_data);

/** Read-ahead area in applying log records to file pages */
constexpr ulint RECV_READ_AHEAD_AREA = 32;

/** The recovery system */
Recv_sys *recv_sys = nullptr;

/** true when applying redo log records during crash recovery; false
otherwise.  Note that this is false while a background thread is
rolling back incomplete transactions. */
bool recv_recovery_on{};

/** true when recv_init_crash_recovery() has been called. */
bool recv_needed_recovery{};

/** true if buf_page_is_corrupted() should check if the log sequence
number (FIL_PAGE_LSN) is in the future.  Initially false, and set by
recv_recovery_from_checkpoint_start_func(). */
bool recv_lsn_checks_on{};

/* User callback function that is called before InnoDB attempts to
rollback incomplete transaction after crash recovery. */
ib_cb_t recv_pre_rollback_hook{};

/** The following counter is used to decide when to print info on log scan */
static ulint recv_scan_print_counter;

/** The type of the previous parsed redo log record */
static ulint recv_previous_parsed_rec_type;

/** The offset of the previous parsed redo log record */
static ulint recv_previous_parsed_rec_offset;

/** The 'multi' flag of the previous parsed redo log record */
static ulint recv_previous_parsed_rec_is_multi;

/** Maximum page number encountered in the redo log */
ulint recv_max_parsed_page_no;

/** This many frames must be left free in the buffer pool when we scan
the log and store the scanned log records in the buffer pool: we will
use these free frames to read in pages when we start applying the
log records to the database.
This is the default value. If the actual size of the buffer pool is
larger than 10 MB we'll set this value to 512. */
ulint recv_n_pool_free_frames;

/** The maximum lsn we see for a page during the recovery process. If this
is bigger than the lsn we are able to scan up to, that is an indication that
the recovery failed and the database may be corrupt. */
lsn_t recv_max_page_lsn;

/**
 * Initialize crash recovery environment. Can be called iff
 * recv_needed_recovery == false.
 * 
 * @param recovery The recovery flag.
 */
static void recv_start_crash_recovery(ib_recovery_t recovery) noexcept;

static std::string to_string(const Recv_sys::Tablespace_map &log_records) noexcept {
  std::string str{};

  for (const auto &[space_id, log_records_map] : log_records) {
    str += "{";
    for (const auto &[page_no, log_record] : log_records_map) {
      str += std::format(
        " space_id: {}, page_no: {}, log_records: {{\n",
        space_id,
        page_no
      );

      str += log_record->to_string() + "},\n";

    }
    str += "},\n";
  }

  str += "},\n";

  return str;
}

void recv_sys_var_init() noexcept {
  ut_a(recv_sys == nullptr);

  recv_lsn_checks_on = false;

  recv_n_pool_free_frames = 256;

  recv_recovery_on = false;

  recv_needed_recovery = false;

  recv_lsn_checks_on = false;

  recv_scan_print_counter = 0;

  recv_previous_parsed_rec_type = 999999;

  recv_previous_parsed_rec_offset = 0;

  recv_previous_parsed_rec_is_multi = 0;

  recv_max_parsed_page_no = 0;

  recv_n_pool_free_frames = 256;

  recv_max_page_lsn = 0;
}

Recv_sys::~Recv_sys() noexcept {
  mutex_free(&m_mutex);

  if (m_heap != nullptr) {
    mem_heap_free(m_heap);
    m_heap = nullptr;
  }

  if (m_buf != nullptr) {
    ut_delete(m_buf);
    m_buf = nullptr;

  }

  if (m_last_block_buf_start != nullptr) {
    mem_free(m_last_block_buf_start);
    m_last_block_buf_start = nullptr;
  }
}

Recv_sys::Recv_sys() noexcept {
  mutex_create(&recv_sys->m_mutex, IF_DEBUG("Recv_sys::m_mutex",) IF_SYNC_DEBUG(SYNC_RECV,) Source_location{});
}

void Recv_sys::open(ulint available_memory) noexcept {
  mutex_enter(&m_mutex);

  ut_a(m_heap == nullptr);
  ut_a(m_log_records.empty());

  m_heap = mem_heap_create_in_buffer(256);

  /* Set appropriate value of recv_n_pool_free_frames. */
  if (srv_buf_pool->get_curr_size() >= (10 * 1024 * 1024)) {
    /* Buffer pool of size greater than 10 MB. */
    recv_n_pool_free_frames = 512;
  }

  ut_a(m_buf == nullptr);
  m_buf = static_cast<byte *>(ut_new(RECV_PARSING_BUF_SIZE));

  ut_a(m_last_block_buf_start == nullptr);
  m_last_block_buf_start = static_cast<byte *>(mem_alloc(2 * IB_FILE_BLOCK_SIZE));

  ut_a(m_last_block_buf_start != nullptr);
  m_last_block = static_cast<byte *>(ut_align(m_last_block_buf_start, IB_FILE_BLOCK_SIZE));

  mutex_exit(&m_mutex);
}

void Recv_sys::clear_log_records() noexcept {
  for (auto &[space, page_recs] : m_log_records) {
    for (auto &[page_no, recs] : page_recs) {
      for (auto recv = recs->m_rec_head; recv != nullptr; recv = recs->m_rec_head) {
        recs->m_rec_head = recv->m_next;
        call_destructor(recv);
      }
      call_destructor(recs);
    }
    page_recs.clear();
  }

  m_log_records.clear();
}

void Recv_sys::reset() noexcept {
  ut_ad(mutex_own(&m_mutex));

  if (m_n_log_records != 0) {
    log_fatal(std::format(
      "{} pages with log records were left unprocessed! Maximum page number with"
      " log records on it {}",
      m_n_log_records,
      recv_max_parsed_page_no
    ));
  }

  clear_log_records();

  mem_heap_empty(m_heap);
}

void Recv_sys::close() noexcept {
  mutex_enter(&m_mutex);

  mem_heap_free(m_heap);
  m_heap = nullptr;

  mem_free(m_last_block_buf_start);
  m_last_block_buf_start = nullptr;

  ut_delete(m_buf);
  m_buf = nullptr;

  clear_log_records();

  mutex_exit(&m_mutex);
}

/**
 * Truncates a log group by erasing log records up to a specified limit.
 *
 * @param group The log group.
 * @param recovered_lsn The LSN up to which recovery has succeeded.
 * @param limit_lsn The limit LSN for recovery. If set to IB_UINT64_T_MAX, erase as much as possible.
 * @param checkpoint_lsn The LSN of the checkpoint from which recovery was started.
 */
static void recv_truncate_group(log_group_t *group, lsn_t recovered_lsn, lsn_t limit_lsn, lsn_t checkpoint_lsn) noexcept {
  auto finish_lsn1 = ut_uint64_align_down(checkpoint_lsn, IB_FILE_BLOCK_SIZE) + log_group_get_capacity(group);
  auto finish_lsn2 = ut_uint64_align_up(recovered_lsn, IB_FILE_BLOCK_SIZE) + recv_sys->m_last_log_buf_size;

  lsn_t finish_lsn;

  if (limit_lsn != IB_UINT64_T_MAX) {
    /* We do not know how far we should erase log records: erase
    as much as possible */

    finish_lsn = finish_lsn1;

  } else {
    /* It is enough to erase the length of the log buffer */
    finish_lsn = finish_lsn1 < finish_lsn2 ? finish_lsn1 : finish_lsn2;
  }

  ut_a(RECV_SCAN_SIZE <= log_sys->buf_size);

  memset(log_sys->buf, '\0', RECV_SCAN_SIZE);

  auto start_lsn = ut_uint64_align_down(recovered_lsn, IB_FILE_BLOCK_SIZE);

  if (start_lsn != recovered_lsn) {
    /* Copy the last incomplete log block to the log buffer and
    edit its data length: */

    memcpy(log_sys->buf, recv_sys->m_last_block, IB_FILE_BLOCK_SIZE);

    log_block_set_data_len(log_sys->buf, ulint(recovered_lsn - start_lsn));
  }

  if (start_lsn < finish_lsn) {
    for (;;) {
      auto end_lsn = start_lsn + RECV_SCAN_SIZE;

      if (end_lsn > finish_lsn) {

        end_lsn = finish_lsn;
      }

      auto len = ulint(end_lsn - start_lsn);

      log_group_write_buf(group, log_sys->buf, len, start_lsn, 0);

      if (end_lsn >= finish_lsn) {

        return;
      }

      memset(log_sys->buf, '\0', RECV_SCAN_SIZE);

      start_lsn = end_lsn;
    }
  }
}

/**
 * Copies the log segment between group->recovered_lsn and recovered_lsn from
 * the most up-to-date log group to group, so that it contains the latest log data.
 * 
 * @param up_to_date_group The most up-to-date log group.
 * @param group The log group to copy to.
 * @param recovered_lsn The LSN up to which recovery has succeeded.
 */
static void recv_copy_group(log_group_t *up_to_date_group, log_group_t *group, lsn_t recovered_lsn) noexcept {
  if (group->scanned_lsn >= recovered_lsn) {

    return;
  }

  ut_a(RECV_SCAN_SIZE <= log_sys->buf_size);

  auto start_lsn = ut_uint64_align_down(group->scanned_lsn, IB_FILE_BLOCK_SIZE);

  for (;;) {
    auto end_lsn = start_lsn + RECV_SCAN_SIZE;

    if (end_lsn > recovered_lsn) {
      end_lsn = ut_uint64_align_up(recovered_lsn, IB_FILE_BLOCK_SIZE);
    }

    log_group_read_log_seg(LOG_RECOVER, log_sys->buf, up_to_date_group, start_lsn, end_lsn);

    auto len = (ulint)(end_lsn - start_lsn);

    log_group_write_buf(group, log_sys->buf, len, start_lsn, 0);

    if (end_lsn >= recovered_lsn) {

      return;
    }

    start_lsn = end_lsn;
  }
}

/**
 * Copies a log segment from the most up-to-date log group to the other log
 * groups, so that they all contain the latest log data. Also writes the info
 * about the latest checkpoint to the groups, and inits the fields in the group
 * memory structs to up-to-date values.
 *
 * @param up_to_date_group The most up-to-date log group.
 */
static void recv_synchronize_groups(log_group_t *up_to_date_group) noexcept {
  auto recovered_lsn = recv_sys->m_recovered_lsn;

  /* Read the last recovered log block to the recovery system buffer:
  the block is always incomplete */

  auto start_lsn = ut_uint64_align_down(recovered_lsn, IB_FILE_BLOCK_SIZE);
  auto end_lsn = ut_uint64_align_up(recovered_lsn, IB_FILE_BLOCK_SIZE);

  ut_a(start_lsn != end_lsn);

  log_group_read_log_seg(LOG_RECOVER, recv_sys->m_last_block, up_to_date_group, start_lsn, end_lsn);

  for (auto group : log_sys->log_groups) {
    if (group != up_to_date_group) {

      /* Copy log data if needed */

      recv_copy_group(group, up_to_date_group, recovered_lsn);
    }

    /* Update the fields in the group struct to correspond to
    recovered_lsn */

    log_group_set_fields(group, recovered_lsn);
  }

  /* Copy the checkpoint info to the groups; remember that we have
  incremented checkpoint_no by one, and the info will not be written
  over the max checkpoint info, thus making the preservation of max
  checkpoint info on disk certain */

  log_groups_write_checkpoint_info();

  mutex_exit(&log_sys->mutex);

  /* Wait for the checkpoint write to complete */
  rw_lock_s_lock(&log_sys->checkpoint_lock);
  rw_lock_s_unlock(&log_sys->checkpoint_lock);

  mutex_enter(&log_sys->mutex);
}

/**
 * Checks if the given buffer containing checkpoint information is consistent.
 *
 * @param buf The buffer containing the checkpoint info.
 * 
 * @return true if the checkpoint is consistent, false otherwise.
 */
static bool recv_check_cp_is_consistent(const byte *buf) noexcept {
  auto fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);

  if ((fold & 0xFFFFFFFFUL) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1)) {
    return false;
  }

  fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);

  if ((fold & 0xFFFFFFFFUL) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2)) {
    return false;
  }

  return true;
}

/**
 * Finds the maximum checkpoint in the log groups and returns the corresponding group and field.
 *
 * @param[out] max_group Pointer to the maximum group found
 * @param[out] max_field Pointer to the maximum field found (LOG_CHECKPOINT_1 or LOG_CHECKPOINT_2)
 * 
 * @return DB_SUCCESS if a valid checkpoint is found, DB_ERROR otherwise
 */
static db_err recv_find_max_checkpoint(log_group_t **max_group, ulint *max_field) noexcept {
  *max_field = 0;
  *max_group = nullptr;

  uint64_t max_no{};
  auto buf = log_sys->checkpoint_buf;

  for (auto group : log_sys->log_groups) {
    group->state = LOG_GROUP_CORRUPTED;

    for (auto field = LOG_CHECKPOINT_1; field <= LOG_CHECKPOINT_2; field += LOG_CHECKPOINT_2 - LOG_CHECKPOINT_1) {

      log_group_read_checkpoint_info(group, field);

      if (recv_check_cp_is_consistent(buf)) {
        group->state = LOG_GROUP_OK;

        group->lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
        group->lsn_offset = mach_read_from_4(buf + LOG_CHECKPOINT_OFFSET);

        auto checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

        if (checkpoint_no >= max_no) {
          *max_group = group;
          *max_field = field;
          max_no = checkpoint_no;
        }
      }
    }
  }

  if (*max_group == nullptr) {

    log_err(
      "No valid checkpoint found. If this error appears when you are"
      " creating an InnoDB database, the problem may be that during"
      " an earlier attempt you managed to create the InnoDB data files,"
      " but log file creation failed. If that is the case, please refer"
      " to the InnoDB website for details"
    );

    return DB_ERROR;
  } else {
    return DB_SUCCESS;
  }
}

/**
 * Checks if the log block checksum is valid.
 * 
 * @param block Pointer to a log block
 * 
 * @return true if the checksum is valid or if the block is in an old format, false otherwise
 */
static bool is_log_block_checksum_ok(const byte *block) noexcept {
  return log_block_calc_checksum(block) == log_block_get_checksum(block);
}

/**
 * Parses and applies a log record body.
 * 
 * Tries to parse a single log record body and also applies it to a page if
 * specified. File ops are parsed, but not applied in this function.
 *
 * @param type The type of the log record.
 * @param ptr Pointer to the buffer containing the log record.
 * @param end_ptr Pointer to the end of the buffer.
 * @param block Pointer to the buffer block or nullptr; if not nullptr,
 *  then the log record is applied to the page, and the log record should be complete then.
 * @param mtr Pointer to the mtr or nullptr; should be non-nullptr if and only if block is non-nullptr.
 * 
 * @return	log record end, nullptr if not a complete record
 */
static byte *recv_parse_or_apply_log_rec_body(
  mlog_type_t type,
  byte *ptr,
  byte *end_ptr,
  buf_block_t *block,
  mtr_t *mtr) noexcept
{
  page_t *page{};
  dict_index_t *index{};
  ut_d(Fil_page_type page_type{FIL_PAGE_TYPE_ALLOCATED});

  ut_ad(!block == !mtr);

  if (block != nullptr) {
    page = block->m_frame;
    ut_d(page_type = srv_fil->page_get_type(page));
  }

  switch (type) {
#ifdef UNIV_LOG_LSN_DEBUG
    case MLOG_LSN:
      /* The LSN is checked in recv_parse_log_rec(). */
      break;
#endif /* UNIV_LOG_LSN_DEBUG */
    case MLOG_1BYTE:
    case MLOG_2BYTES:
    case MLOG_4BYTES:
    case MLOG_8BYTES:
#ifdef UNIV_DEBUG
      if (page != nullptr && page_type == FIL_PAGE_TYPE_ALLOCATED && end_ptr >= ptr + 2) {
        /* It is OK to set FIL_PAGE_TYPE and certain list node fields on an empty page. Any other
        write is not OK. */

        /* NOTE: There may be bogus assertion failures for dict_hdr_create(), trx_rseg_header_create(),
        trx_sys_create_doublewrite_buf(), and trx_sysf_create(). These are only called during database creation. */

       ulint  offs = mach_read_from_2(ptr);

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
#endif /* UNIV_DEBUG */
      ptr = mlog_parse_nbytes(type, ptr, end_ptr, page);
      break;
    case MLOG_REC_INSERT:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, &index)) != nullptr) {
        ptr = page_cur_parse_insert_rec(false, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_REC_CLUST_DELETE_MARK:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, &index)) != nullptr) {
        ptr = btr_cur_parse_del_mark_set_clust_rec(ptr, end_ptr, page, index);
      }
    case MLOG_REC_SEC_DELETE_MARK:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);
      ptr = btr_cur_parse_del_mark_set_sec_rec(ptr, end_ptr, page);
      break;
    case MLOG_REC_UPDATE_IN_PLACE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, &index)) != nullptr) {
        ptr = btr_cur_parse_update_in_place(ptr, end_ptr, page, index);
      }
      break;
    case MLOG_LIST_END_DELETE:
    case MLOG_LIST_START_DELETE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, &index)) != nullptr) {
        ptr = page_parse_delete_rec_list(type, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_LIST_END_COPY_CREATED:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, &index)) != nullptr) {
        ptr = page_parse_copy_rec_list_to_created_page(ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_PAGE_REORGANIZE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, &index)) != nullptr) {
        ptr = btr_parse_page_reorganize(ptr, end_ptr, index, block, mtr);
      }
      break;
    case MLOG_PAGE_CREATE:
      /* Allow anything in page_type when creating a page. */
      ptr = page_parse_create(ptr, end_ptr, block, mtr);
      break;
    case MLOG_UNDO_INSERT:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = trx_undo_parse_add_undo_rec(ptr, end_ptr, page);
      break;
    case MLOG_UNDO_ERASE_END:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = trx_undo_parse_erase_page_end(ptr, end_ptr, page, mtr);
      break;
    case MLOG_UNDO_INIT:
      /* Allow anything in page_type when creating a page. */
      ptr = trx_undo_parse_page_init(ptr, end_ptr, page, mtr);
      break;
    case MLOG_UNDO_HDR_DISCARD:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = trx_undo_parse_discard_latest(ptr, end_ptr, page, mtr);
      break;
    case MLOG_UNDO_HDR_CREATE:
    case MLOG_UNDO_HDR_REUSE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = trx_undo_parse_page_header(type, ptr, end_ptr, page, mtr);
      break;
    case MLOG_REC_MIN_MARK:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);
      ptr = btr_parse_set_min_rec_mark(ptr, end_ptr, page, mtr);
      break;
    case MLOG_REC_DELETE:
      ut_ad(!page || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, &index)) != nullptr) {
        ptr = page_cur_parse_delete_rec(ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_INIT_FILE_PAGE:
      /* Allow anything in page_type when creating a page. */
      ptr = fsp_parse_init_file_page(ptr, end_ptr, block);
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
    dict_mem_index_free(index);
    dict_mem_table_free(index->table);
  }

  return ptr;
}

/**
 * Gets the hashed file address struct for a page.
 * 
 * @param space The space id.
 * @param page_no The page number.
 * 
 * @return	file address struct, nullptr if not found from the hash table
 */
static Page_log_records *recv_get_log_record(space_id_t space, page_no_t page_no) noexcept {
  auto space_it = recv_sys->m_log_records.find(space);

  if (space_it == recv_sys->m_log_records.end()) {
    return nullptr;
  }

  auto &page_map = space_it->second;
  auto page_it = page_map.find(page_no);

  return page_it == page_map.end() ? nullptr : page_it->second;
}

void Recv_sys::add_log_record(
  mlog_type_t type,
  space_id_t space,
  page_no_t page_no,
  byte *body,
  byte *rec_end,
  lsn_t start_lsn,
  lsn_t end_lsn) noexcept
{
  if (srv_fil->tablespace_deleted_or_being_deleted_in_mem(space, -1)) {
    /* The tablespace does not exist any more: do not store the
    log record */

    return;
  }

  auto recv = reinterpret_cast<Log_record *>(mem_heap_alloc(m_heap, sizeof(Log_record)));

  new (recv) Log_record();

  recv->m_type = type;
  recv->m_end_lsn = end_lsn;
  recv->m_len = rec_end - body;
  recv->m_start_lsn = start_lsn;

  auto log_record = recv_get_log_record(space, page_no);

  if (log_record == nullptr) {
    log_record = reinterpret_cast<Page_log_records *>(mem_heap_alloc(m_heap, sizeof(Page_log_records)));

    new (log_record) Page_log_records();

    log_record->m_rec_head = recv;
    log_record->m_rec_tail = recv;

    m_log_records[space][page_no] = log_record;

    ++m_n_log_records;
  } else {
    log_record->m_rec_tail->m_next = recv;
  }

  ut_a(recv->m_next == nullptr);
  log_record->m_rec_tail = recv;

  auto prev_field = &recv->m_data;

  /* Store the log record body in chunks of less than UNIV_PAGE_SIZE:
  m_heap grows into the buffer pool, and bigger chunks could not
  be allocated */

  while (rec_end > body) {

    auto len = ulint(rec_end - body);

    if (len > RECV_DATA_BLOCK_SIZE) {
      len = RECV_DATA_BLOCK_SIZE;
    }

    auto recv_data = reinterpret_cast<Log_record_data *>(mem_heap_alloc(m_heap, sizeof(Log_record_data) + len));

    *prev_field = recv_data;

    memcpy(recv_data + 1, body, len);

    prev_field = &recv_data->m_next;

    body += len;
  }

  *prev_field = nullptr;
}

/**
 * Copies the log record body from recv to buf.
 * @param buf The buffer to copy the log record body to.
 * @param recv The log record.
 */
static void recv_data_copy_to_buf(byte *buf, Log_record *recv) noexcept {
  ulint part_len;

  auto len = recv->m_len;
  auto recv_data = recv->m_data;

  while (len > 0) {
    if (len > RECV_DATA_BLOCK_SIZE) {
      part_len = RECV_DATA_BLOCK_SIZE;
    } else {
      part_len = len;
    }

    memcpy(buf, reinterpret_cast<byte *>(recv_data) + sizeof(Log_record_data), part_len);
    buf += part_len;
    len -= part_len;

    recv_data = recv_data->m_next;
  }
}

void recv_recover_page(bool just_read_in, buf_block_t *block) noexcept {
  mtr_t mtr;

  mutex_enter(&recv_sys->m_mutex);

  if (!recv_sys->m_apply_log_recs) {

    /* Log records should not be applied now */

    mutex_exit(&recv_sys->m_mutex);

    return;
  }

  auto log_record = recv_get_log_record(block->get_space(), block->get_page_no());

  if (log_record == nullptr || log_record->m_state == RECV_BEING_PROCESSED || log_record->m_state == RECV_PROCESSED) {

    mutex_exit(&recv_sys->m_mutex);

    return;
  }

  log_record->m_state = RECV_BEING_PROCESSED;

  mutex_exit(&recv_sys->m_mutex);

  mtr_start(&mtr);
  mtr_set_log_mode(&mtr, MTR_LOG_NONE);

  auto page = block->m_frame;

  if (just_read_in) {
    /* Move the ownership of the x-latch on the page to
    this OS thread, so that we can acquire a second
    x-latch on it.  This is needed for the operations to
    the page to pass the debug checks. */

    rw_lock_x_lock_move_ownership(&block->m_rw_lock);
  }

  Buf_pool::Request req {
    .m_rw_latch  = RW_X_LATCH,
    .m_guess = block,
    .m_mode = BUF_KEEP_OLD,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = &mtr
  };

  auto success = srv_buf_pool->try_get_known_nowait(req);
  ut_a(success);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(lock, SYNC_NO_ORDER_CHECK));

  /* Read the newest modification lsn from the page */
  lsn_t page_lsn = mach_read_from_8(page + FIL_PAGE_LSN);

  /* It may be that the page has been modified in the buffer
  pool: read the newest modification lsn there */

  auto page_newest_lsn = buf_page_get_newest_modification(&block->m_page);

  if (page_newest_lsn) {

    page_lsn = page_newest_lsn;
  }

  lsn_t end_lsn{};
  lsn_t start_lsn{};
  bool modification_to_page{};

  ut_a(log_record->m_rec_head != nullptr);

  for (auto recv{log_record->m_rec_head}; recv != nullptr; recv = recv->m_next) {  
    byte *buf;
    byte *ptr{};

    end_lsn = recv->m_end_lsn;

    if (recv->m_len > RECV_DATA_BLOCK_SIZE) {
      /* We have to copy the record body to a separate buffer */

      ptr = buf = static_cast<byte *>(mem_alloc(recv->m_len));

      recv_data_copy_to_buf(buf, recv);

    } else {
      buf = reinterpret_cast<byte *>(recv->m_data) + sizeof(Log_record_data);
    }

    if (recv->m_type == MLOG_INIT_FILE_PAGE) {
      page_lsn = page_newest_lsn;

      memset(FIL_PAGE_LSN + page, 0, 8);
      memset(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + page, 0, 8);
    }

    if (recv->m_start_lsn >= page_lsn) {

      if (!modification_to_page) {

        modification_to_page = true;
        start_lsn = recv->m_start_lsn;
      }

      recv_parse_or_apply_log_rec_body(recv->m_type, buf, buf + recv->m_len, block, &mtr);

      end_lsn = recv->m_start_lsn + recv->m_len;

      mach_write_to_8(FIL_PAGE_LSN + page, end_lsn);
      mach_write_to_8(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + page, end_lsn);
    }

    if (recv->m_len > RECV_DATA_BLOCK_SIZE) {
      ut_a(ptr != nullptr && ptr == buf);
      mem_free(ptr);
    }
  }

  mutex_enter(&recv_sys->m_mutex);

  ut_a(recv_sys->m_n_log_records > 0);
  --recv_sys->m_n_log_records;

  if (recv_max_page_lsn < page_lsn) {
    recv_max_page_lsn = page_lsn;
  }

  log_record->m_state = RECV_PROCESSED;

  mutex_exit(&recv_sys->m_mutex);

  if (modification_to_page) {
    ut_a(block != nullptr);

    srv_buf_pool->m_flusher->recv_note_modification(block, start_lsn, end_lsn);
  }

  /* Make sure that committing mtr does not change the modification
  lsn values of page */

  mtr.modifications = false;

  mtr_commit(&mtr);
}

/**
 * Reads in pages which have hashed log records, from an area around a given
 * page number.
 * 
 * @param space The space id.
 * @param start_page_no The start page number.
 * 
 * @return	number of pages found
 */
static ulint recv_read_in_area(space_id_t space, page_no_t start_page_no) noexcept {
  std::array<page_no_t, RECV_READ_AHEAD_AREA> page_nos;
  const auto low_limit = start_page_no - (start_page_no % RECV_READ_AHEAD_AREA);

  ulint n = 0;

  for (auto page_no  = low_limit; page_no < low_limit + RECV_READ_AHEAD_AREA; ++page_no) {
    auto log_record = recv_get_log_record(space, page_no);

    if (log_record != nullptr && !srv_buf_pool->peek(space, page_no)) {

      mutex_enter(&recv_sys->m_mutex);

      if (log_record->m_state == RECV_NOT_PROCESSED) {
        log_record->m_state = RECV_BEING_READ;

        page_nos[n] = page_no;

        n++;
      }

      mutex_exit(&recv_sys->m_mutex);
    }
  }

  buf_read_recv_pages(false, space, page_nos.data(), n);

  return n;
}

void recv_apply_log_recs(bool flush_and_free_pages) noexcept {
  for (;;) {
    mutex_enter(&recv_sys->m_mutex);

    if (!recv_sys->m_apply_batch_on) {
      break;
    }

    mutex_exit(&recv_sys->m_mutex);

    os_thread_sleep(100000);
  }

  recv_sys->m_apply_log_recs = true;
  recv_sys->m_apply_batch_on = true;

  const ulint n_total{ recv_sys->m_n_log_records };

  ulint n_recs{};
  bool printed_header{};

  for (const auto &[space_id, log_records_map] : recv_sys->m_log_records) {

    for (const auto &[page_no, log_record] : log_records_map) {

      if (log_record->m_state == RECV_NOT_PROCESSED) {

        if (!printed_header) {
          log_info("Starting an apply batch of log records to the database");
          log_info_hdr("Progress in percents: ");
          printed_header = true;
        }

        mutex_exit(&recv_sys->m_mutex);

        if (srv_buf_pool->peek(space_id, page_no)) {

          mtr_t mtr;

          mtr_start(&mtr);

          Buf_pool::Request req {
            .m_rw_latch = RW_X_LATCH,
            .m_page_id = { space_id, page_no },
            .m_mode = BUF_GET,
            .m_file = __FILE__,
            .m_line = __LINE__,
            .m_mtr = &mtr
          };

          auto block = srv_buf_pool->get(req, nullptr);
          buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_NO_ORDER_CHECK));

          recv_recover_page(false, block);

          mtr_commit(&mtr);
        } else {
          recv_read_in_area(space_id, page_no);
        }

        mutex_enter(&recv_sys->m_mutex);
      }

      const auto pct { (n_recs * 100) / n_total };

      if (printed_header && pct != ((n_recs + 1) * 100) / n_total) {
        log_info_msg(std::format("{} ", pct));
      }

      ++n_recs;
    }
  }

  /* Wait until all the pages have been processed */
  while (recv_sys->m_n_log_records != 0) {

    mutex_exit(&recv_sys->m_mutex);

    os_thread_sleep(100000);

    mutex_enter(&recv_sys->m_mutex);
  }

  if (printed_header) {
    log_info_msg("\n");
  }

  if (flush_and_free_pages) {
    /* Flush all the file pages to disk and invalidate them in the buffer pool */
    mutex_exit(&recv_sys->m_mutex);
    mutex_exit(&log_sys->mutex);

    auto n_pages = srv_buf_pool->m_flusher->batch(BUF_FLUSH_LIST, ULINT_MAX, IB_UINT64_T_MAX);
    ut_a(n_pages != ULINT_UNDEFINED);

    srv_buf_pool->m_flusher->wait_batch_end(BUF_FLUSH_LIST);

    srv_buf_pool->invalidate();

    mutex_enter(&log_sys->mutex);
    mutex_enter(&recv_sys->m_mutex);
  }

  recv_sys->m_apply_log_recs = false;
  recv_sys->m_apply_batch_on = false;

  recv_sys->reset();

  if (printed_header) {
    log_info("Apply batch completed");
  }

  mutex_exit(&recv_sys->m_mutex);
}

/**
 * Tries to parse a single log record and returns its length.
 * 
 * @param ptr Pointer to the buffer.
 * @param end_ptr Pointer to the end of the buffer.
 * @param type Pointer to the type of the log record.
 * @param space Pointer to the space id.
 * @param page_no Pointer to the page number.
 * @param body Pointer to the log record body.
 * 
 * @return	length of the record, or 0 if the record was not complete
 */
static ulint recv_parse_log_rec(
  byte *ptr,
  byte *end_ptr,
  mlog_type_t *type,
  space_id_t *space,
  page_no_t *page_no,
  byte **body) noexcept
{
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

    *space = ULINT_UNDEFINED - 1; /* For debugging */

    return 1;
  }

  auto new_ptr = mlog_parse_initial_log_record(ptr, end_ptr, type, space, page_no);

  *body = new_ptr;

  if (unlikely(!new_ptr)) {

    return 0;
  }

#ifdef UNIV_LOG_LSN_DEBUG
  if (*type == MLOG_LSN) {
    auto lsn = lsn_t(*space) << 32 | *page_no;
    ut_a(lsn == recv_sys->m_recovered_lsn);
  }
#endif /* UNIV_LOG_LSN_DEBUG */

  new_ptr = recv_parse_or_apply_log_rec_body(*type, new_ptr, end_ptr, nullptr, nullptr);

  if (unlikely(new_ptr == nullptr)) {

    return 0;
  }

  if (*page_no > recv_max_parsed_page_no) {
    recv_max_parsed_page_no = *page_no;
  }

  return new_ptr - ptr;
}

/**
 * Calculates the new value for lsn when more data is added to the log.
 * 
 * @param lsn The old lsn.
 * @param len The length of the data added, excluding log block headers.
 */
static lsn_t recv_calc_lsn_on_data_add(lsn_t lsn, uint64_t len) noexcept {
  auto frag_len = (ulint(lsn) % IB_FILE_BLOCK_SIZE) - LOG_BLOCK_HDR_SIZE;

  ut_ad(frag_len < IB_FILE_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE);
  auto lsn_len = ulint(len);

  lsn_len += (lsn_len + frag_len) / (IB_FILE_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE) *
             (LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE);

  return lsn + lsn_len;
}

/**
 * Prints diagnostic info of corrupt log.
 * 
 * @param[in] ptr Pointer to the corrupt log record.
 * @param[in] type The type of the log record.
 * @param[in] space The space id.
 * @param[in] page_no The page number.
 */
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
    (ulong) recv_previous_parsed_rec_type,
    recv_previous_parsed_rec_is_multi,
    ptr - recv_sys->m_buf,
    recv_previous_parsed_rec_offset
  ));

  if (ulint(ptr - recv_sys->m_buf + 100) > recv_previous_parsed_rec_offset && ulint(ptr - recv_sys->m_buf + 100 - recv_previous_parsed_rec_offset) < 200000) {
    log_err(
      "Hex dump of corrupt log starting 100 bytes before the start"
      " of the previous log rec, and ending 100 bytes after the start"
      " of the corrupt rec:"
    );

    log_warn_buf(recv_sys->m_buf + recv_previous_parsed_rec_offset - 100, ptr - recv_sys->m_buf + 200 - recv_previous_parsed_rec_offset);
  }

  if (!srv_force_recovery) {
    log_fatal("Set innodb_force_recovery to ignore this error.");
  }

  log_warn(
    "The log file may have been corrupt and it s possible that the log scan did not proceed"
    " far enough in recovery! Please run CHECK TABLE on your InnoDB tables to check that"
    " they are ok! If the engine crashes after this recovery, check the InnoDB website"
    " for details about forcing recovery."
  );
}

/**
 * Parses log records from a buffer and stores them to a hash table to wait
 * merging to file pages.
 * 
 * @param store_to_hash true if the records should be stored to the hash table;
 *  this is set to false if just debug checking is needed
 * @return	currently always returns false
 */
static bool recv_parse_log_recs(bool store_to_hash) noexcept {
  ulint len;
  lsn_t old_lsn;
  ulint total_len;
  mlog_type_t type;
  byte *body;
  space_id_t space;
  page_no_t page_no;
  lsn_t new_recovered_lsn;

  ut_ad(mutex_own(&log_sys->mutex));
  ut_ad(recv_sys->m_parse_start_lsn != 0);

  for (;;) {
    auto ptr = recv_sys->m_buf + recv_sys->m_recovered_offset;
    auto end_ptr = recv_sys->m_buf + recv_sys->m_len;

    if (ptr == end_ptr) {

      return false;
    }

    auto single_rec = ulint(*ptr & MLOG_SINGLE_REC_FLAG);

    if (single_rec || *ptr == MLOG_DUMMY_RECORD) {
      /* The mtr only modified a single page, or this is a file op */

      old_lsn = recv_sys->m_recovered_lsn;

      /* Try to parse a log record, fetching its type, space id,
      page no, and a pointer to the body of the log record */

      len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);

      if (len == 0 || recv_sys->m_found_corrupt_log) {
        if (recv_sys->m_found_corrupt_log) {

          recv_report_corrupt_log(ptr, type, space, page_no);
        }

        return false;
      }

      new_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

      if (new_recovered_lsn > recv_sys->m_scanned_lsn) {
        /* The log record filled a log block, and we require
        that also the next log block should have been scanned
        in */

        return false;
      }

      recv_previous_parsed_rec_type = (ulint)type;
      recv_previous_parsed_rec_offset = recv_sys->m_recovered_offset;
      recv_previous_parsed_rec_is_multi = 0;

      recv_sys->m_recovered_offset += len;
      recv_sys->m_recovered_lsn = new_recovered_lsn;

      if (type == MLOG_DUMMY_RECORD) {
        /* Do nothing */
 
      } else if (!store_to_hash) {
        /* In debug checking, update a replicate page according to
        the log record, and check that it becomes identical with the
        original page */

      } else if (type == MLOG_FILE_CREATE || type == MLOG_FILE_RENAME || type == MLOG_FILE_DELETE) {
        ut_a(space != SYS_TABLESPACE);

        /* In normal crash recovery we do not try to replay file operations */
#ifdef UNIV_LOG_LSN_DEBUG
      } else if (type == MLOG_LSN) {
        /* Do not add these records to the hash table. The page number and space
        id fields are misused for something else. */
#endif /* UNIV_LOG_LSN_DEBUG */
      } else {
        recv_sys->add_log_record(type, space, page_no, body, ptr + len, old_lsn, recv_sys->m_recovered_lsn);
      }
    } else {
      /* Check that all the records associated with the single mtr
      are included within the buffer */

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

          /* Found the end mark for the records */

          break;
        }
      }

      new_recovered_lsn = recv_calc_lsn_on_data_add(recv_sys->m_recovered_lsn, total_len);

      if (new_recovered_lsn > recv_sys->m_scanned_lsn) {
        /* The log record filled a log block, and we require
        that also the next log block should have been scanned
        in */

        return false;
      }

      /* Add all the records to the hash table */

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

          /* Found the end mark for the records */

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

/**
 * Adds data from a new log block to the parsing buffer of recv_sys if
 * recv_sys->m_parse_start_lsn is non-zero.
 * 
 * @param log_block Pointer to the log block.
 * @param scanned_lsn The LSN up to which the data has been scanned.
 * 
 * @return	true if more data added
 */
static bool recv_sys_add_to_parsing_buf(const byte *log_block, lsn_t scanned_lsn) noexcept {
  ulint more_len;

  ut_ad(scanned_lsn >= recv_sys->m_scanned_lsn);

  if (!recv_sys->m_parse_start_lsn) {
    /* Cannot start parsing yet because no start point for
    it found */

    return false;
  }

  auto data_len = log_block_get_data_len(log_block);

  if (recv_sys->m_parse_start_lsn >= scanned_lsn) {

    return false;

  } else if (recv_sys->m_scanned_lsn >= scanned_lsn) {

    return false;

  } else if (recv_sys->m_parse_start_lsn > recv_sys->m_scanned_lsn) {
    more_len = (ulint)(scanned_lsn - recv_sys->m_parse_start_lsn);
  } else {
    more_len = (ulint)(scanned_lsn - recv_sys->m_scanned_lsn);
  }

  if (more_len == 0) {

    return false;
  }

  ut_ad(data_len >= more_len);

  auto start_offset = data_len - more_len;

  if (start_offset < LOG_BLOCK_HDR_SIZE) {
    start_offset = LOG_BLOCK_HDR_SIZE;
  }

  auto end_offset = data_len;

  if (end_offset > IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
    end_offset = IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
  }

  ut_ad(start_offset <= end_offset);

  if (start_offset < end_offset) {
    memcpy(recv_sys->m_buf + recv_sys->m_len, log_block + start_offset, end_offset - start_offset);

    recv_sys->m_len += end_offset - start_offset;

    ut_a(recv_sys->m_len <= RECV_PARSING_BUF_SIZE);
  }

  return true;
}

/**
 * Moves the parsing buffer data left to the buffer start.
 */
static void recv_sys_justify_left_parsing_buf() noexcept {
  memmove(recv_sys->m_buf, recv_sys->m_buf + recv_sys->m_recovered_offset, recv_sys->m_len - recv_sys->m_recovered_offset);

  recv_sys->m_len -= recv_sys->m_recovered_offset;

  recv_sys->m_recovered_offset = 0;
}

/**
 * Scans log from a buffer and stores new log data to the parsing buffer.
 * Parses and stores the log records if new data found.  This function will
 * apply log records automatically when the stored records table becomes full.
 * 
 * 
 * @param[in] recovery           Recovery flag
 * @param[in] available_memory   We let the storage of recs to grow to this
 *                               size, at the maximum
 * @param[in] store_to_hash      true if the records should be stored;
 *                               this is set to false if just debug
 * 				                       checking is needed
 * @param[in] buf                Buffer containing a log segment or garbage
 * @param[in] len                Buffer length
 * @param[in] start_lsn          Buffer start lsn
 * @param[in,out] contiguous_lsn It is known that all log groups contain
 *                               contiguouslog data up to this lsn
 * @param[out] group_scanned_lsn Scanning succeeded up to this lsn
 * 
 * @return true if limit_lsn has been reached, or not able to scan any more in this log group.
 */
static bool recv_scan_log_recs(
  ib_recovery_t recovery,
  ulint available_memory,
  bool store_to_hash,
  const byte *buf,
  ulint len,
  lsn_t start_lsn,
  lsn_t *contiguous_lsn,
  lsn_t *group_scanned_lsn
) noexcept {

  ut_ad(len >= IB_FILE_BLOCK_SIZE);
  ut_ad(len % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(start_lsn % IB_FILE_BLOCK_SIZE == 0);

  auto log_block = buf;
  bool finished = false;
  auto more_data = false;
  auto scanned_lsn = start_lsn;

  do {
    auto no = log_block_get_hdr_no(log_block);
    const auto checksum_ok{is_log_block_checksum_ok(log_block)};
    const auto block_no{log_block_convert_lsn_to_no(scanned_lsn)};

    if (no != block_no || !checksum_ok) {
      if (no == block_no && !checksum_ok) {
        log_warn(std::format(
          "Log block no {} at lsn {} has ok header, but checksum field contains {}, should be {}",
          no,
          scanned_lsn,
          log_block_get_checksum(log_block),
          log_block_calc_checksum(log_block)
        ));
      }

      /* Garbage or an incompletely written log block */

      finished = true;

      break;
    }

    if (log_block_get_flush_bit(log_block)) {
      /* This block was a start of a log flush operation: we know that the previous flush
      operation must have been completed for all log groups before this block can have been
      flushed to any of the groups. Therefore, we know that log data is contiguous up to
      scanned_lsn in all non-corrupt log groups. */

      if (scanned_lsn > *contiguous_lsn) {
        *contiguous_lsn = scanned_lsn;
      }
    }

    auto data_len = log_block_get_data_len(log_block);

    if ((store_to_hash || data_len == IB_FILE_BLOCK_SIZE) &&
        scanned_lsn + data_len > recv_sys->m_scanned_lsn &&
        recv_sys->m_scanned_checkpoint_no > 0 &&
        log_block_get_checkpoint_no(log_block) < recv_sys->m_scanned_checkpoint_no &&
        recv_sys->m_scanned_checkpoint_no - log_block_get_checkpoint_no(log_block) > 0x80000000UL) {

      /* Garbage from a log buffer flush which was made before the most recent database recovery */

      finished = true;
      break;
    }

    if (recv_sys->m_parse_start_lsn == 0 && log_block_get_first_rec_group(log_block) > 0) {

      /* We found a point from which to start the parsing of log records */

      recv_sys->m_parse_start_lsn = scanned_lsn + log_block_get_first_rec_group(log_block);
      recv_sys->m_scanned_lsn = recv_sys->m_parse_start_lsn;
      recv_sys->m_recovered_lsn = recv_sys->m_parse_start_lsn;
    }

    scanned_lsn += data_len;

    if (scanned_lsn > recv_sys->m_scanned_lsn) {

      /* We have found more entries. If this scan is of startup type, we must initiate crash recovery
      environment before parsing these log records. */

      if (!recv_needed_recovery) {

        log_info("Log scan progressed past the checkpoint lsn ", recv_sys->m_scanned_lsn);
        recv_start_crash_recovery(recovery);
      }

      /* We were able to find more log data: add it to the parsing buffer if parse_start_lsn is already
      non-zero */

      if (recv_sys->m_len + 4 * IB_FILE_BLOCK_SIZE >= RECV_PARSING_BUF_SIZE) {
        log_err("Log parsing buffer overflow. Recovery may have failed!");

        recv_sys->m_found_corrupt_log = true;

        if (!srv_force_recovery) {
          log_fatal("Set innodb_force_recovery to ignore this error.");
        }

      } else if (!recv_sys->m_found_corrupt_log) {
        more_data = recv_sys_add_to_parsing_buf(log_block, scanned_lsn);
      }

      recv_sys->m_scanned_lsn = scanned_lsn;
      recv_sys->m_scanned_checkpoint_no = log_block_get_checkpoint_no(log_block);
    }

    if (data_len < IB_FILE_BLOCK_SIZE) {
      /* Log data for this group ends here */

      finished = true;
      break;

    } else {
      log_block += IB_FILE_BLOCK_SIZE;

    }
  } while (log_block < buf + len && !finished);

  *group_scanned_lsn = scanned_lsn;

  if (recv_needed_recovery) {
    recv_scan_print_counter++;

    if (finished || (recv_scan_print_counter % 80 == 0)) {

      log_info("Doing recovery: scanned up to log sequence number ", *group_scanned_lsn);
    }
  }

  if (more_data && !recv_sys->m_found_corrupt_log) {
    /* Try to parse more log records */

    recv_parse_log_recs(store_to_hash);

    if (store_to_hash && mem_heap_get_size(recv_sys->m_heap) > available_memory) {

      /* Hash table of log records has grown too big: empty it. */

      recv_apply_log_recs(true);
    }

    if (recv_sys->m_recovered_offset > RECV_PARSING_BUF_SIZE / 4) {
      /* Move parsing buffer data to the buffer start */

      recv_sys_justify_left_parsing_buf();
    }
  }

  return finished;
}

/**
 * Scans log from a buffer and stores new log data to the parsing buffer.
 * Parses and hashes the log records if new data found.
 * 
 * @param recovery The recovery flag.
 * @param group The log group.
 * @param contiguous_lsn The LSN up to which the log data is known to be contiguous.
 * @param group_scanned_lsn The LSN up to which the log data has been scanned.
 * 
 */
static void recv_group_scan_log_recs(
  ib_recovery_t recovery,
  log_group_t *group,
  lsn_t *contiguous_lsn,
  lsn_t *group_scanned_lsn) noexcept
{

  bool finished = false;
  auto start_lsn = *contiguous_lsn;

  while (!finished) {
    auto end_lsn = start_lsn + RECV_SCAN_SIZE;

    log_group_read_log_seg(LOG_RECOVER, log_sys->buf, group, start_lsn, end_lsn);

    finished = recv_scan_log_recs(
      recovery,
      (srv_buf_pool->m_curr_size - recv_n_pool_free_frames) * UNIV_PAGE_SIZE,
      true,
      log_sys->buf,
      RECV_SCAN_SIZE,
      start_lsn,
      contiguous_lsn,
      group_scanned_lsn
    );
    start_lsn = end_lsn;
  }
}

static void recv_start_crash_recovery(ib_recovery_t recovery)  noexcept {
  ut_a(!recv_needed_recovery);

  recv_needed_recovery = true;

  log_warn("Database was not shut down normally! Starting crash recovery.");

  log_warn("Reading tablespace information from the .ibd files...");

  /* Recursively scan to a depth of 2. */
  srv_fil->load_single_table_tablespaces(srv_data_home, recovery, 2);

  /* If we are using the doublewrite method, we will check if there are half-written
  pages in data files, and restore them from the doublewrite buffer if possible */

  if (recovery < IB_RECOVERY_NO_LOG_REDO) {
    log_warn("Restoring possible half-written data pages from the doublewrite buffer...");
    trx_sys_doublewrite_init_or_restore_pages(true);
  }
}

/**
 * Compare the checkpoint lsn with the lsn recorded in the data files
 * and start crash recovery if required.
 * 
 * @param recovery The recovery flag.
 * @param checkpoint_lsn The checkpoint LSN.
 * @param max_flushed_lsn The maximum flushed LSN from data files.
 */
static void recv_init_crash_recovery(ib_recovery_t recovery, lsn_t checkpoint_lsn, lsn_t max_flushed_lsn) noexcept {
  /* NOTE: we always do a 'recovery' at startup, but only if there is something wrong we
  will print a message to the user about recovery: */

  if (checkpoint_lsn != max_flushed_lsn) {

    if (checkpoint_lsn < max_flushed_lsn) {
      log_warn(std::format(
        "##########################################################\n"
        "                         WARNING!\n"
        " The log sequence number in 'system.idb' file is higher than\n"
        " the log sequence number in the ib_logfiles! Are you sure you\n"
        " are using the right ib_logfiles to start up the database?\n"
        " Log sequence number in ib_logfiles is {}, max log sequence\n"
        " number stamped in 'system.idb; file header is and {}\n"
        "##########################################################",
        checkpoint_lsn,
        max_flushed_lsn
      ));
    }

    if (!recv_needed_recovery) {
      log_err(
        "The log sequence number in 'system.idb' file does not match"
        " the log sequence number in the ib_logfiles!");

      recv_start_crash_recovery(recovery);
    }
  }

  if (!recv_needed_recovery) {
    /* Init the doublewrite buffer memory structure */
    trx_sys_doublewrite_init_or_restore_pages(false);
  }
}

db_err recv_recovery_from_checkpoint_start(ib_recovery_t recovery, lsn_t max_flushed_lsn) noexcept {
  ut_a(recv_sys == nullptr);

  recv_sys = static_cast<Recv_sys *>(mem_alloc(sizeof(Recv_sys)));

  new (recv_sys) Recv_sys();

  recv_sys->open(srv_buf_pool->get_curr_size());

  if (recovery >= IB_RECOVERY_NO_LOG_REDO) {
    log_warn("The user has set IB_RECOVERY_NO_LOG_REDO on. Skipping log redo");

    return DB_SUCCESS;
  }

  log_info("Check if we need to do recovery, max flushed LSN is ", max_flushed_lsn);

  recv_recovery_on = true;

  recv_sys->m_limit_lsn = IB_UINT64_T_MAX;

  mutex_enter(&log_sys->mutex);

  /* Look for the latest checkpoint from any of the log groups */

  ulint max_cp_field;
  log_group_t *max_cp_group;

  auto err = recv_find_max_checkpoint(&max_cp_group, &max_cp_field);

  if (err != DB_SUCCESS) {

    mutex_exit(&log_sys->mutex);

    return err;
  }

  log_group_read_checkpoint_info(max_cp_group, max_cp_field);

  auto buf = log_sys->checkpoint_buf;

  lsn_t checkpoint_lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
  auto checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

  log_info(std::format("Found max checkpoint lsn: {}, checkpoint no: {}", checkpoint_lsn, checkpoint_no));

  /* Start reading the log groups from the checkpoint lsn up. The
  variable contiguous_lsn contains an lsn up to which the log is
  known to be contiguously written to all log groups. */

  recv_sys->m_parse_start_lsn = checkpoint_lsn;
  recv_sys->m_scanned_lsn = checkpoint_lsn;
  recv_sys->m_scanned_checkpoint_no = 0;
  recv_sys->m_recovered_lsn = checkpoint_lsn;

  srv_start_lsn = checkpoint_lsn;

  auto up_to_date_group = max_cp_group;
  auto contiguous_lsn = ut_uint64_align_down(recv_sys->m_scanned_lsn, IB_FILE_BLOCK_SIZE);

  ut_ad(RECV_SCAN_SIZE <= log_sys->buf_size);

  lsn_t group_scanned_lsn{};

  for (auto group : log_sys->log_groups) {
    auto old_scanned_lsn = recv_sys->m_scanned_lsn;

    recv_group_scan_log_recs(recovery, group, &contiguous_lsn, &group_scanned_lsn);

    group->scanned_lsn = group_scanned_lsn;

    if (old_scanned_lsn < group_scanned_lsn) {
      /* We found a more up-to-date group */

      up_to_date_group = group;
    }
  }

  recv_init_crash_recovery(recovery, checkpoint_lsn, max_flushed_lsn);

  /* We currently have only one log group */
  if (group_scanned_lsn < checkpoint_lsn) {
    log_err(std::format(
      "We were only able to scan the log up to {}, but a checkpoint"
      " was at {}. It is possible that the database is now corrupt!",
      group_scanned_lsn,
      checkpoint_lsn
    ));
  }

  if (group_scanned_lsn < recv_max_page_lsn) {
    log_err(std::format(
      "We were only able to scan the log up to {}"
      " but a database page a had an lsn {}. It is possible that the"
      " database is now corrupt!",
      group_scanned_lsn,
      recv_max_page_lsn
    ));
  }

  if (recv_sys->m_recovered_lsn < checkpoint_lsn) {

    mutex_exit(&log_sys->mutex);

    ut_a(recv_sys->m_recovered_lsn >= LSN_MAX);

    return DB_SUCCESS;
  }

  /* Synchronize the uncorrupted log groups to the most up-to-date log
  group; we also copy checkpoint info to groups */

  log_sys->next_checkpoint_lsn = checkpoint_lsn;
  log_sys->next_checkpoint_no = checkpoint_no + 1;

  recv_synchronize_groups(up_to_date_group);

  if (!recv_needed_recovery) {
    ut_a(checkpoint_lsn == recv_sys->m_recovered_lsn);
  } else {
    srv_start_lsn = recv_sys->m_recovered_lsn;
  }

  log_sys->lsn = recv_sys->m_recovered_lsn;

  memcpy(log_sys->buf, recv_sys->m_last_block, IB_FILE_BLOCK_SIZE);

  log_sys->buf_free = ulint(log_sys->lsn % IB_FILE_BLOCK_SIZE);
  log_sys->buf_next_to_write = log_sys->buf_free;
  log_sys->written_to_some_lsn = log_sys->lsn;
  log_sys->written_to_all_lsn = log_sys->lsn;

  log_sys->last_checkpoint_lsn = checkpoint_lsn;

  log_sys->next_checkpoint_no = checkpoint_no + 1;

  mutex_enter(&recv_sys->m_mutex);

  recv_sys->m_apply_log_recs = true;

  mutex_exit(&recv_sys->m_mutex);

  mutex_exit(&log_sys->mutex);

  recv_lsn_checks_on = true;

  /* The database is now ready to start almost normal processing of user
  transactions: transaction rollbacks and the application of the log
  records in the hash table can be run in background. */

  return DB_SUCCESS;
}

void recv_recovery_from_checkpoint_finish(ib_recovery_t recovery) noexcept {
  /* Apply the hashed log records to the respective file pages */

  if (recovery < IB_RECOVERY_NO_LOG_REDO) {

    recv_apply_log_recs(false);
  }

  if (recv_sys->m_found_corrupt_log) {

    log_warn(
      "WARNING: the log file may have been corrupt and it"
      " is possible that the log scan or parsing did not proceed"
      " far enough in recovery. Please run CHECK TABLE"
      " on your InnoDB tables to check that they are ok!"
      " It may be safest to recover your InnoDB database from"
      " a backup!"
    );
  }

  /* Free the resources of the recovery system */

  recv_recovery_on = false;

  ut_a(recv_sys != nullptr);

  recv_sys->close();

  call_destructor(recv_sys);

  mem_free(recv_sys);

  recv_sys = nullptr;

  /* Free up the flush_rbt. */
  srv_buf_pool->m_flusher->free_flush_rbt();

  /* Roll back any recovered data dictionary transactions, so
  that the data dictionary tables will be free of any locks.
  The data dictionary latch should guarantee that there is at
  most one data dictionary transaction active at a time. */
  trx_rollback_or_clean_recovered(false);
}

void recv_recovery_rollback_active() noexcept {
  /* This is required to set the compare context before recovery, also
  it's a one-shot. We can safely set it to nullptr after calling it. */
  if (recv_pre_rollback_hook != nullptr) {
    recv_pre_rollback_hook();
    recv_pre_rollback_hook = nullptr;
  }

#ifdef UNIV_SYNC_DEBUG
  /* Wait for a while so that created threads have time to suspend
  themselves before we switch the latching order checks on */
  os_thread_sleep(1000000);

  /* Switch latching order checks on in sync0sync.c */
  sync_order_checks_on = true;
#endif

  ddl_drop_all_temp_indexes(ib_recovery_t(srv_force_recovery));
  ddl_drop_all_temp_tables(ib_recovery_t(srv_force_recovery));

  if (srv_force_recovery < IB_RECOVERY_NO_TRX_UNDO) {
    /* Rollback the uncommitted transactions which have no user
    session */

    os_thread_create(trx_rollback_or_clean_all_recovered, nullptr, nullptr);
  }
}

void recv_reset_logs(lsn_t lsn, bool new_logs_created) noexcept {
  ut_ad(mutex_own(&log_sys->mutex));

  log_sys->lsn = ut_uint64_align_up(lsn, IB_FILE_BLOCK_SIZE);

  for (auto group : log_sys->log_groups) {
    group->lsn = log_sys->lsn;
    group->lsn_offset = LOG_FILE_HDR_SIZE;

    if (!new_logs_created) {
      recv_truncate_group(group, group->lsn, group->lsn, group->lsn);
    }
  }

  log_sys->buf_next_to_write = 0;
  log_sys->written_to_some_lsn = log_sys->lsn;
  log_sys->written_to_all_lsn = log_sys->lsn;

  log_sys->next_checkpoint_no = 0;
  log_sys->last_checkpoint_lsn = 0;

  log_block_init(log_sys->buf, log_sys->lsn);
  log_block_set_first_rec_group(log_sys->buf, LOG_BLOCK_HDR_SIZE);

  log_sys->buf_free = LOG_BLOCK_HDR_SIZE;
  log_sys->lsn += LOG_BLOCK_HDR_SIZE;

  mutex_exit(&log_sys->mutex);

  /* Reset the checkpoint fields in logs */

  log_make_checkpoint_at(IB_UINT64_T_MAX, true);
  log_make_checkpoint_at(IB_UINT64_T_MAX, true);

  mutex_enter(&log_sys->mutex);
}

std::string to_string(Recv_addr_state s) noexcept {
  switch (s) {
    case RECV_UNKNOWN:
      return "RECV_UNKNOWN";
    case RECV_NOT_PROCESSED:
      return "RECV_NOT_PROCESSED";
    case RECV_BEING_READ:
      return "RECV_BEING_READ";
    case RECV_BEING_PROCESSED:
      return "RECV_BEING_PROCESSED";
    case RECV_PROCESSED:
      return "RECV_PROCESSED";
    default:
      log_fatal("Unknown recv_addr_state: {}", s);
  }
}

std::string Log_record_data::to_string() const noexcept {
  return std::format("FIXME");
}

std::string Log_record::to_string() const noexcept {
  return std::format(
    "m_type: {}, : {}, m_start_lsn: {}, m_end_lsn: {}, data: {}",
    mlog_type_str(m_type),
    m_len,
    m_start_lsn,
    m_end_lsn,
    m_data->to_string()
  );
}

std::string Page_log_records::to_string() const noexcept {
  std::string str{};

  for (auto lr = m_rec_head; lr != nullptr; lr = lr->m_next) {
    str += lr->to_string();
    str += ", ";
  } 

  return str;
}

std::string Recv_sys::to_string() const noexcept {
  std::string str = std::format(
    "m_parse_start_lsn: {}, m_scanned_lsn: {}, m_recovered_lsn: {}, m_scanned_checkpoint_no: {},"
    " m_recovered_offset: {}, m_len: {}, m_limit_lsn: {}, m_apply_log_recs: {}, m_apply_batch_on:"
    " {}, m_found_corrupt_log: {}, m_n_log_records: {}",
    m_parse_start_lsn,
    m_scanned_lsn,
    m_recovered_lsn,
    m_scanned_checkpoint_no,
    m_recovered_offset,
    m_len,
    m_limit_lsn,
    m_apply_log_recs,
    m_apply_batch_on,
    m_found_corrupt_log,
    m_n_log_records
  );

  return str + ::to_string(m_log_records);
}
