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
#include "buf0dblwr.h"
#include "buf0flu.h"
#include "buf0rea.h"
#include "ddl0ddl.h"
#include "fil0fil.h"
#include "mem0mem.h"
#include "mtr0log.h"
#include "mtr0mtr.h"
#include "page0cur.h"
#include "srv0srv.h"
#include "sync0sync.h"

#include "trx0roll.h"
#include "trx0undo.h"
#include "trx0undo_rec.h"

/** Log records are stored in the hash table in chunks at most of this size;
this must be less than UNIV_PAGE_SIZE as it is stored in the buffer pool */
constexpr ulint RECV_DATA_BLOCK_SIZE = MEM_MAX_ALLOC_IN_BUF - sizeof(LogRecordData);

/** Read-ahead area in applying log records to file pages */
constexpr ulint RECV_READ_AHEAD_AREA = 32;





/* User callback function that is called before InnoDB attempts to
rollback incomplete transaction after crash recovery. */








/**
 * Initialize crash recovery environment. Can be called iff
 * sys.m_recv->m_needed_recovery == false.
 *
 * @param[in,out] dblwr Doublewrite instance to use for recovery
 * @param[in] recovery The recovery flag.
 */
void recv_start_crash_recovery(System &sys) noexcept;

static std::string to_string(const RecvSys::Tablespace_map &log_records) noexcept {
  std::string str{};

  for (const auto &[space_id, log_records_map] : log_records) {
    str += "{";
    for (const auto &[page_no, log_record] : log_records_map) {
      str += std::format(" space_id: {}, page_no: {}, log_records: {{\n", space_id, page_no);

      str += log_record->to_string() + "},\n";
    }
    str += "},\n";
  }

  str += "},\n";

  return str;
}

RecvSys::~RecvSys() noexcept {
  mutex_free(&m_mutex);
  cleanup_resources();
}

void RecvSys::cleanup_resources() noexcept {
  if (m_heap != nullptr) {
    mem_heap_free(m_heap);
    m_heap = nullptr;
  }

  if (m_buf != nullptr) {
    ut_delete(m_buf);
    m_buf = nullptr;
  }
}

RecvSys::RecvSys(System &sys) noexcept : m_sys(sys) {
  mutex_create(&m_mutex, IF_DEBUG("RecvSys::m_mutex", )  Current_location());
}

void RecvSys::open(ulint available_memory) noexcept {
  mutex_enter(&m_mutex);

  ut_a(m_heap == nullptr);
  ut_a(m_log_records.empty());

  m_heap = mem_heap_create_in_buffer(256);

  /* Frames kept free for the pages that records are applied to: reads of
  those pages wait while half of them are taken by pending reads. */
  m_n_pool_free_frames = 256;

  if (m_sys.m_buf_pool->get_curr_size() >= (10 * 1024 * 1024)) {
    /* Buffer pool of size greater than 10 MB. */
    m_n_pool_free_frames = 512;
  }

  ut_a(m_buf == nullptr);
  m_buf = static_cast<byte *>(ut_new(RECV_PARSING_BUF_SIZE));

  mutex_exit(&m_mutex);
}

void RecvSys::clear_log_records() noexcept {
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

void RecvSys::reset() noexcept {
  ut_ad(mutex_own(&m_mutex));

  if (m_n_log_records != 0) {
    log_fatal(std::format(
      "{} pages with log records were left unprocessed! Maximum page number with"
      " log records on it {}",
      m_n_log_records,
      m_max_parsed_page_no
    ));
  }

  clear_log_records();

  mem_heap_empty(m_heap);
}

void RecvSys::close() noexcept {
  mutex_enter(&m_mutex);
  clear_log_records();
  cleanup_resources();
  mutex_exit(&m_mutex);
}

/**
 * Parses and applies a log record body.
 *
 * Tries to parse a single log record body and also applies it to a page if
 * specified. File ops are parsed, but not applied in this function.
 *
 * @param[in] type The type of the log record.
 * @param[in] ptr Pointer to the buffer containing the log record.
 * @param[in] end_ptr Pointer to the end of the buffer.
 * @param[in] block Pointer to the buffer block or nullptr; if not nullptr,
 *  then the log record is applied to the page, and the log record should be complete then.
 * @param[in] mtr Pointer to the mtr or nullptr; should be non-nullptr if and only if block is non-nullptr.
 *
 * @return	log record end, nullptr if not a complete record
 */
byte *recv_parse_or_apply_log_rec_body(System &sys, MlogType type, byte *ptr, byte *end_ptr, BufBlock *block, Mtr *mtr) noexcept {
  page_t *page{};
  Index *index{};
  ut_d(FilPageType page_type{FIL_PAGE_TYPE_ALLOCATED});

  ut_ad(!block == !mtr);

  if (block != nullptr) {
    page = block->m_frame;
    ut_d(page_type = Fil::page_get_type(page));
  }

  switch (type) {
    case MLOG_1BYTE:
    case MLOG_2BYTES:
    case MLOG_4BYTES:
    case MLOG_8BYTES:
#ifdef UNIV_DEBUG
      if (page != nullptr && page_type == FIL_PAGE_TYPE_ALLOCATED && end_ptr >= ptr + 2) {
        /* It is OK to set FIL_PAGE_TYPE and certain list node fields on an empty page. Any other
        write is not OK. */

        /* NOTE: There may be bogus assertion failures for dict_hdr_create(), trx_rseg_header_create(),
        and trx_sysf_create(). These are only called during database creation. */

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
#endif /* UNIV_DEBUG */
      ptr = mlog_parse_nbytes(sys, type, ptr, end_ptr, page);
      break;
    case MLOG_REC_INSERT:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = PageCursor::parse_insert_rec(sys, false, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_REC_CLUST_DELETE_MARK:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = BtreeCursor::parse_del_mark_set_clust_rec(sys, ptr, end_ptr, page, index);
      }
      break;
    case MLOG_REC_SEC_DELETE_MARK:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);
      ptr = BtreeCursor::parse_del_mark_set_sec_rec(ptr, end_ptr, page);
      break;
    case MLOG_REC_UPDATE_IN_PLACE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = BtreeCursor::parse_update_in_place(sys, ptr, end_ptr, page, index);
      }
      break;
    case MLOG_LIST_END_DELETE:
    case MLOG_LIST_START_DELETE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = page_parse_delete_rec_list(sys, type, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_LIST_END_COPY_CREATED:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = PageCursor::parse_copy_rec_list_to_created_page(sys, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_PAGE_REORGANIZE:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = sys.m_btree->parse_page_reorganize(ptr, end_ptr, index, block, mtr);
      }
      break;
    case MLOG_PAGE_CREATE:
      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        /* Allow anything in page_type when creating a page. */
        ptr = page_parse_create(ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_UNDO_INSERT:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = TrxUndoRecord::parse_add_undo_rec(ptr, end_ptr, page);
      break;
    case MLOG_UNDO_ERASE_END:
      ut_ad(page == nullptr || page_type == FIL_PAGE_TYPE_UNDO_LOG);
      ptr = TrxUndoRecord::parse_erase_page_end(ptr, end_ptr, page, mtr);
      break;
    case MLOG_UNDO_INIT:
      /* Allow anything in page_type when creating a page. */
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
      ptr = sys.m_btree->parse_set_min_rec_mark(ptr, end_ptr, page, mtr);
      break;
    case MLOG_REC_DELETE:
      ut_ad(!page || page_type == FIL_PAGE_TYPE_INDEX);

      if ((ptr = mlog_parse_index(ptr, end_ptr, index)) != nullptr) {
        ptr = PageCursor::parse_delete_rec(sys, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_INIT_FILE_PAGE:
      /* Allow anything in page_type when creating a page. */
      ptr = sys.m_fsp->parse_init_file_page(ptr, end_ptr, block);
      break;
    case MLOG_WRITE_STRING:
      ut_ad(!page || page_type != FIL_PAGE_TYPE_ALLOCATED);
      ptr = mlog_parse_string(sys, ptr, end_ptr, page);
      break;
    case MLOG_FILE_CREATE:
    case MLOG_FILE_RENAME:
    case MLOG_FILE_DELETE:
      ptr = sys.m_fil->op_log_parse_or_replay(ptr, end_ptr, type, 0, 0);
      break;
    default:
      ptr = nullptr;
      sys.m_recv->m_found_corrupt_log = true;
  }

  // Get rid of the dummy index, it's too cosstly to create and destroy so frequently.
  if (index != nullptr) {
    auto table = index->m_table;
    Index::destroy(index, Current_location());
    Table::destroy(table, Current_location());
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
static PageLogRecords *recv_get_log_record(System &sys, space_id_t space, page_no_t page_no) noexcept {
  auto space_it = sys.m_recv->m_log_records.find(space);

  if (space_it == sys.m_recv->m_log_records.end()) {
    return nullptr;
  }

  auto &page_map = space_it->second;
  auto page_it = page_map.find(page_no);

  return page_it == page_map.end() ? nullptr : page_it->second;
}

void RecvSys::add_log_record(
  MlogType type, space_id_t space, page_no_t page_no, byte *body, byte *rec_end, lsn_t start_lsn, lsn_t end_lsn
) noexcept {
  if (m_sys.m_fil->tablespace_deleted_or_being_deleted_in_mem(space, -1)) {
    /* The tablespace does not exist any more: do not store the
    log record */

    return;
  }

  auto recv = reinterpret_cast<LogRecord *>(mem_heap_alloc(m_heap, sizeof(LogRecord)));

  new (recv) LogRecord();

  recv->m_type = type;
  recv->m_end_lsn = end_lsn;
  recv->m_len = rec_end - body;
  recv->m_start_lsn = start_lsn;

  auto log_record = recv_get_log_record(m_sys, space, page_no);

  if (log_record == nullptr) {
    log_record = reinterpret_cast<PageLogRecords *>(mem_heap_alloc(m_heap, sizeof(PageLogRecords)));

    new (log_record) PageLogRecords();

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
    const auto len = std::min(ulint(rec_end - body), ulint(RECV_DATA_BLOCK_SIZE));
    auto recv_data = reinterpret_cast<LogRecordData *>(mem_heap_alloc(m_heap, sizeof(LogRecordData) + len));
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
static void recv_data_copy_to_buf(byte *buf, LogRecord *recv) noexcept {
  auto len = recv->m_len;
  auto recv_data = recv->m_data;

  while (len > 0) {
    const auto part_len = (len > RECV_DATA_BLOCK_SIZE) ? RECV_DATA_BLOCK_SIZE : len;
    memcpy(buf, reinterpret_cast<byte *>(recv_data) + sizeof(LogRecordData), part_len);
    buf += part_len;
    len -= part_len;
    recv_data = recv_data->m_next;
  }
}

void recv_recover_page(System &sys, bool just_read_in, BufBlock *block) noexcept {
  Mtr mtr;

  mutex_enter(&sys.m_recv->m_mutex);

  if (!sys.m_recv->m_apply_log_recs) {

    /* Log records should not be applied now */

    mutex_exit(&sys.m_recv->m_mutex);

    return;
  }

  auto log_record = recv_get_log_record(sys, block->get_space(), block->get_page_no());

  if (log_record == nullptr || log_record->m_state == RECV_BEING_PROCESSED || log_record->m_state == RECV_PROCESSED) {

    mutex_exit(&sys.m_recv->m_mutex);

    return;
  }

  log_record->m_state = RECV_BEING_PROCESSED;

  mutex_exit(&sys.m_recv->m_mutex);

  mtr.start();

  {
    auto old_mode = mtr.set_log_mode(MTR_LOG_NONE);
    ut_a(old_mode == MTR_LOG_ALL);
  }

  auto page = block->m_frame;

  if (just_read_in) {
    /* Move the ownership of the x-latch on the page to
    this OS thread, so that we can acquire a second
    x-latch on it.  This is needed for the operations to
    the page to pass the debug checks. */

    rw_lock_x_lock_move_ownership(&block->m_rw_lock);
  }

  BufPool::Request req{
    .m_rw_latch = RW_X_LATCH, .m_guess = block, .m_mode = BUF_KEEP_OLD, .m_file = __FILE__, .m_line = __LINE__, .m_mtr = &mtr
  };

  auto success = sys.m_buf_pool->try_get_known_nowait(req);
  ut_a(success);

  buf_block_dbg_add_level();

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
      ptr = buf = static_cast<byte *>(mem_alloc(recv->m_len));
      recv_data_copy_to_buf(buf, recv);
    } else {
      buf = reinterpret_cast<byte *>(recv->m_data) + sizeof(LogRecordData);
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

      recv_parse_or_apply_log_rec_body(sys, recv->m_type, buf, buf + recv->m_len, block, &mtr);

      end_lsn = recv->m_start_lsn + recv->m_len;

      mach_write_to_8(FIL_PAGE_LSN + page, end_lsn);
      mach_write_to_8(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_CHKSUM + page, end_lsn);
    }

    if (ptr != nullptr) {
      mem_free(ptr);
    }
  }

  mutex_enter(&sys.m_recv->m_mutex);

  ut_a(sys.m_recv->m_n_log_records > 0);
  --sys.m_recv->m_n_log_records;

  if (sys.m_recv->m_max_page_lsn < page_lsn) {
  }

  log_record->m_state = RECV_PROCESSED;

  mutex_exit(&sys.m_recv->m_mutex);

  if (modification_to_page) {
    ut_a(block != nullptr);

    sys.m_buf_pool->m_flusher->recv_note_modification(block, start_lsn, end_lsn);
  }

  /* Make sure that committing mtr does not change the modification
  lsn values of page */

  mtr.m_modifications = false;

  mtr.commit(sys);
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
static ulint recv_read_in_area(System &sys, space_id_t space, page_no_t start_page_no) noexcept {
  std::array<page_no_t, RECV_READ_AHEAD_AREA> page_nos;
  const auto low_limit = start_page_no - (start_page_no % RECV_READ_AHEAD_AREA);

  ulint n = 0;

  for (auto page_no = low_limit; page_no < low_limit + RECV_READ_AHEAD_AREA; ++page_no) {
    auto log_record = recv_get_log_record(sys, space, page_no);

    if (log_record != nullptr && !sys.m_buf_pool->peek(PageId(space, page_no))) {

      mutex_enter(&sys.m_recv->m_mutex);

      if (log_record->m_state == RECV_NOT_PROCESSED) {
        log_record->m_state = RECV_BEING_READ;

        page_nos[n] = page_no;

        n++;
      }

      mutex_exit(&sys.m_recv->m_mutex);
    }
  }

  buf_read_recv_pages(sys, false, PageId(space, 0), page_nos.data(), n);

  return n;
}

void recv_apply_log_recs(System &sys, bool flush_and_free_pages) noexcept {
  for (;;) {
    mutex_enter(&sys.m_recv->m_mutex);

    if (!sys.m_recv->m_apply_batch_on) {
      break;
    }

    mutex_exit(&sys.m_recv->m_mutex);

    os_thread_sleep(100000);
  }

  sys.m_recv->m_apply_log_recs = true;
  sys.m_recv->m_apply_batch_on = true;

  const ulint n_total{sys.m_recv->m_n_log_records};

  ulint n_recs{};
  bool printed_header{};

  for (const auto &[space_id, log_records_map] : sys.m_recv->m_log_records) {

    for (const auto &[page_no, log_record] : log_records_map) {

      if (log_record->m_state == RECV_NOT_PROCESSED) {

        if (!printed_header) {
          log_info("Starting an apply batch of log records to the database");
          log_info_hdr("Progress in percents: ");
          printed_header = true;
        }

        mutex_exit(&sys.m_recv->m_mutex);

        if (sys.m_buf_pool->peek(PageId(space_id, page_no))) {

          Mtr mtr;

          mtr.start();

          BufPool::Request req{
            .m_rw_latch = RW_X_LATCH,
            .m_page_id = {space_id, page_no},
            .m_mode = BUF_GET,
            .m_file = __FILE__,
            .m_line = __LINE__,
            .m_mtr = &mtr
          };

          auto block = sys.m_buf_pool->get(req, nullptr);
          buf_block_dbg_add_level();

          recv_recover_page(sys, false, block);

          mtr.commit(sys);
        } else {
          recv_read_in_area(sys, space_id, page_no);
        }

        mutex_enter(&sys.m_recv->m_mutex);
      }

      const auto pct{(n_recs * 100) / n_total};

      if (printed_header && pct != ((n_recs + 1) * 100) / n_total) {
        log_info_msg(std::format("{} ", pct));
      }

      ++n_recs;
    }
  }

  /* Wait until all the pages have been processed */
  while (sys.m_recv->m_n_log_records != 0) {

    mutex_exit(&sys.m_recv->m_mutex);

    os_thread_sleep(100000);

    mutex_enter(&sys.m_recv->m_mutex);
  }

  if (printed_header) {
    log_info_msg("\n");
  }

  if (flush_and_free_pages) {
    /* Flush all the file pages to disk and invalidate them in the buffer pool */
    mutex_exit(&sys.m_recv->m_mutex);
    sys.m_log->release();

    auto n_pages = sys.m_buf_pool->m_flusher->batch(BUF_FLUSH_LIST, ULINT_MAX, IB_UINT64_T_MAX);
    ut_a(n_pages != ULINT_UNDEFINED);

    sys.m_buf_pool->m_flusher->wait_batch_end(BUF_FLUSH_LIST);

    sys.m_buf_pool->invalidate();

    sys.m_log->acquire();
    mutex_enter(&sys.m_recv->m_mutex);
  }

  sys.m_recv->m_apply_log_recs = false;
  sys.m_recv->m_apply_batch_on = false;

  sys.m_recv->reset();

  if (printed_header) {
    log_info("Apply batch completed");
  }

  mutex_exit(&sys.m_recv->m_mutex);
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
static ulint recv_parse_log_rec(System &sys, 
  byte *ptr, byte *end_ptr, MlogType *type, space_id_t *space, page_no_t *page_no, byte **body
) noexcept {
  *body = nullptr;

  if (ptr == end_ptr) {

    return 0;
  }

  if (*ptr == MLOG_MULTI_REC_END) {

    *type = static_cast<MlogType>(*ptr);

    return 1;
  }

  if (*ptr == MLOG_DUMMY_RECORD) {
    *type = static_cast<MlogType>(*ptr);

    *space = ULINT32_UNDEFINED - 1; /* For debugging */

    return 1;
  }

  auto new_ptr = mlog_parse_initial_log_record(ptr, end_ptr, type, space, page_no);

  *body = new_ptr;

  if (unlikely(!new_ptr)) {

    return 0;
  }

  new_ptr = recv_parse_or_apply_log_rec_body(sys, *type, new_ptr, end_ptr, nullptr, nullptr);

  if (unlikely(new_ptr == nullptr)) {

    return 0;
  }

  if (*page_no > sys.m_recv->m_max_parsed_page_no) {
  }

  return new_ptr - ptr;
}

/**
 * Calculates the new value for lsn when more data is added to the log.
 *
 * @param lsn The old lsn.
 * @param len The length of the data added.
 */
static lsn_t recv_calc_lsn_on_data_add(lsn_t lsn, uint64_t len) noexcept {
  /* A WAL LSN counts log bytes only: the block headers are not part of it. */
  return lsn + len;
}

/**
 * Prints diagnostic info of corrupt log.
 *
 * @param[in] ptr Pointer to the corrupt log record.
 * @param[in] type The type of the log record.
 * @param[in] space The space id.
 * @param[in] page_no The page number.
 */
static void recv_report_corrupt_log(System &sys, byte *ptr, byte type, space_id_t space, page_no_t page_no) noexcept {
  log_err(std::format(
    "############### CORRUPT LOG RECORD FOUND\n"
    "Log record type {}, space id {}, page number {}\n"
    "Log parsing proceeded successfully up to {}\n"
    "Previous log record type {}, is multi {}\n"
    "Recv offset {}, prev {}",
    (ulong)type,
    space,
    page_no,
    sys.m_recv->m_recovered_lsn,
    (ulong)sys.m_recv->m_previous_parsed_rec_type,
    sys.m_recv->m_previous_parsed_rec_is_multi,
    ptr - sys.m_recv->m_buf,
    sys.m_recv->m_previous_parsed_rec_offset
  ));

  const ulint at = ptr - sys.m_recv->m_buf;
  const ulint prev = sys.m_recv->m_previous_parsed_rec_offset;

  if (at >= prev && at - prev < 200000) {
    /* From 100 bytes before the previous record to 100 bytes after this one,
    inside the parsing buffer. */
    const ulint from = prev > 100 ? prev - 100 : 0;
    const ulint to = std::min<ulint>(at + 100, sys.m_recv->m_len);

    log_err(
      "Hex dump of corrupt log starting 100 bytes before the start"
      " of the previous log rec, and ending 100 bytes after the start"
      " of the corrupt rec:"
    );

    log_warn_buf(sys.m_recv->m_buf + from, to - from);
  }

  if (!sys.m_config.m_recv.m_force_recovery) {
    log_fatal("Set innodb_force_recovery to ignore this error.");
  }

  log_warn(
    "The log may have been corrupt and it is possible that the log scan did not proceed"
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
bool recv_parse_log_recs(System &sys, bool store_to_hash) noexcept {
  ulint len;
  lsn_t old_lsn;
  ulint total_len;
  MlogType type;
  byte *body;
  space_id_t space;
  page_no_t page_no;
  lsn_t new_recovered_lsn;

  ut_ad(mutex_own(&sys.m_log->m_mutex));
  ut_ad(sys.m_recv->m_parse_start_lsn != 0);

  for (;;) {
    auto ptr = sys.m_recv->m_buf + sys.m_recv->m_recovered_offset;
    auto end_ptr = sys.m_recv->m_buf + sys.m_recv->m_len;

    if (ptr == end_ptr) {

      return false;
    }

    auto single_rec = ulint(*ptr & MLOG_SINGLE_REC_FLAG);

    if (single_rec || *ptr == MLOG_DUMMY_RECORD) {
      /* The mtr only modified a single page, or this is a file op */

      old_lsn = sys.m_recv->m_recovered_lsn;

      /* Try to parse a log record, fetching its type, space id,
      page no, and a pointer to the body of the log record */

      len = recv_parse_log_rec(sys, ptr, end_ptr, &type, &space, &page_no, &body);

      if (len == 0 || sys.m_recv->m_found_corrupt_log) {
        if (sys.m_recv->m_found_corrupt_log) {
          recv_report_corrupt_log(sys, ptr, type, space, page_no);
        }
        return false;
      }

      new_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

      if (new_recovered_lsn > sys.m_recv->m_scanned_lsn) {
        /* The record group continues past the log read so far. */

        return false;
      }


      sys.m_recv->m_previous_parsed_rec_type = type;
      sys.m_recv->m_previous_parsed_rec_offset = sys.m_recv->m_recovered_offset;
      sys.m_recv->m_previous_parsed_rec_is_multi = 0;

      sys.m_recv->m_recovered_offset += len;
      sys.m_recv->m_recovered_lsn = new_recovered_lsn;

      if (type == MLOG_DUMMY_RECORD) {
        /* Do nothing */

      } else if (!store_to_hash) {
        /* In debug checking, update a replicate page according to
        the log record, and check that it becomes identical with the
        original page */

      } else if (type == MLOG_FILE_CREATE || type == MLOG_FILE_RENAME || type == MLOG_FILE_DELETE) {
        ut_a(space != SYS_TABLESPACE);

        /* In normal crash recovery we do not try to replay file operations */
      } else {
        sys.m_recv->add_log_record(type, space, page_no, body, ptr + len, old_lsn, sys.m_recv->m_recovered_lsn);
      }
    } else {
      /* Check that all the records associated with the single mtr
      are included within the buffer */

      total_len = 0;

      for (;;) {
        auto len = recv_parse_log_rec(sys, ptr, end_ptr, &type, &space, &page_no, &body);

        if (len == 0 || sys.m_recv->m_found_corrupt_log) {
          if (sys.m_recv->m_found_corrupt_log) {
            recv_report_corrupt_log(sys, ptr, type, space, page_no);
          }
          return false;
        }


        sys.m_recv->m_previous_parsed_rec_type = type;
        sys.m_recv->m_previous_parsed_rec_offset = sys.m_recv->m_recovered_offset + total_len;
        sys.m_recv->m_previous_parsed_rec_is_multi = 1;

        total_len += len;

        ptr += len;

        if (type == MLOG_MULTI_REC_END) {

          /* Found the end mark for the records */

          break;
        }
      }

      new_recovered_lsn = recv_calc_lsn_on_data_add(sys.m_recv->m_recovered_lsn, total_len);

      if (new_recovered_lsn > sys.m_recv->m_scanned_lsn) {
        /* The record group continues past the log read so far. */

        return false;
      }

      /* Add all the records to the hash table */

      ptr = sys.m_recv->m_buf + sys.m_recv->m_recovered_offset;

      for (;;) {
        old_lsn = sys.m_recv->m_recovered_lsn;

        len = recv_parse_log_rec(sys, ptr, end_ptr, &type, &space, &page_no, &body);

        if (sys.m_recv->m_found_corrupt_log) {
          recv_report_corrupt_log(sys, ptr, type, space, page_no);
        }

        ut_a(len != 0);
        ut_a(((ulint)*ptr & MLOG_SINGLE_REC_FLAG) == 0);

        sys.m_recv->m_recovered_offset += len;
        sys.m_recv->m_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

        if (type == MLOG_MULTI_REC_END) {

          /* Found the end mark for the records */

          break;
        }

        if (store_to_hash) {
          sys.m_recv->add_log_record(type, space, page_no, body, ptr + len, old_lsn, new_recovered_lsn);
        }

        ptr += len;
      }
    }
  }
}

/**
 * Moves the parsing buffer data left to the buffer start.
 */
static void recv_sys_justify_left_parsing_buf(System &sys) noexcept {
  memmove(sys.m_recv->m_buf, sys.m_recv->m_buf + sys.m_recv->m_recovered_offset, sys.m_recv->m_len - sys.m_recv->m_recovered_offset);

  sys.m_recv->m_len -= sys.m_recv->m_recovered_offset;

  sys.m_recv->m_recovered_offset = 0;
}

void recv_start_crash_recovery(System &sys) noexcept {
  ut_a(!sys.m_recv->m_needed_recovery);

  sys.m_recv->m_needed_recovery = true;

  log_warn("Database was not shut down normally! Starting crash recovery.");
}

/**
 * Takes log read from the WAL: adds it to the parsing buffer, parses the
 * complete records in it and stores them for applying. Applies the stored
 * records when they take more memory than recovery may use.
 *
 * @param[in] data              Log bytes, they follow the bytes passed before.
 * @param[in] len               Number of bytes.
 * @param[in] available_memory  The stored records may use this much memory.
 *
 * @return false if the log is corrupt: stop reading.
 */
static bool recv_scan_log(System &sys, const byte *data, ulint len, ulint available_memory) noexcept {
  auto &recv = *sys.m_recv;

  if (!recv.m_needed_recovery) {
    log_info("Log scan progressed past the checkpoint lsn ", recv.m_scanned_lsn);
    recv_start_crash_recovery(sys);
  }

  if (recv.m_len + len > RECV_PARSING_BUF_SIZE) {
    recv_sys_justify_left_parsing_buf(sys);
  }

  /* A record group, an mtr's log, is at most a few pages. */
  if (recv.m_len + len > RECV_PARSING_BUF_SIZE) {
    log_err("Log parsing buffer overflow. Recovery may have failed!");

    recv.m_found_corrupt_log = true;

    return false;
  }

  memcpy(recv.m_buf + recv.m_len, data, len);

  recv.m_len += len;
  recv.m_scanned_lsn += len;

  (void) recv_parse_log_recs(sys, true);

  if (recv.m_found_corrupt_log) {
    return false;
  }

  if (mem_heap_get_size(recv.m_heap) > available_memory) {
    recv_apply_log_recs(sys, true);
  }

  if (recv.m_recovered_offset > RECV_PARSING_BUF_SIZE / 4) {
    recv_sys_justify_left_parsing_buf(sys);
  }

  return true;
}

DbErr recv_recovery_from_checkpoint_start(System &sys, lsn_t max_flushed_lsn) noexcept {
  sys.m_recv->open(sys.m_buf_pool->get_curr_size());

  if (sys.m_config.m_recv.m_force_recovery >= IB_RECOVERY_NO_LOG_REDO) {
    log_warn("The user has set IB_RECOVERY_NO_LOG_REDO on. Skipping log redo");

    return DB_SUCCESS;
  }

  log_info("Check if we need to do recovery, max flushed LSN is ", max_flushed_lsn);

  auto &recv = *sys.m_recv;
  auto &log = *sys.m_log;

  lsn_t checkpoint_lsn{};
  uint64_t checkpoint_no{};

  /* A database whose WAL kept no checkpoints can only have been shut down
  cleanly: its data files are stamped with the LSN they were flushed to, and
  its WAL is not readable. */
  const auto have_checkpoint = log.read_checkpoint(checkpoint_lsn, checkpoint_no);

  if (!have_checkpoint) {
    checkpoint_lsn = max_flushed_lsn;
  }

  if (checkpoint_lsn == 0) {
    log_err(
      "No valid checkpoint found. If this error appears when you are"
      " creating an InnoDB database, the problem may be that during"
      " an earlier attempt you managed to create the InnoDB data files,"
      " but the WAL was not created."
    );

    return DB_ERROR;
  }

  log_info(std::format("Found max checkpoint lsn: {}, checkpoint no: {}", checkpoint_lsn, checkpoint_no));

  recv.m_recovery_on = true;
  recv.m_scanned_lsn = checkpoint_lsn;
  recv.m_recovered_lsn = checkpoint_lsn;
  recv.m_parse_start_lsn = checkpoint_lsn;

  sys.m_srv->m_start_lsn = checkpoint_lsn;

  log.acquire();

  if (have_checkpoint) {
    /* The parsed records may use the buffer pool but for the frames kept
    free for the pages they are applied to. */
    const auto available_memory = (sys.m_buf_pool->m_curr_size - recv.m_n_pool_free_frames) * UNIV_PAGE_SIZE;

    const auto end_lsn = LogWalBackend::read(log.wal_config(), checkpoint_lsn, [&](const byte *data, ulint len) {
      return recv_scan_log(sys, data, len, available_memory);
    });

    if (recv.m_needed_recovery) {
      log_info(std::format(
        "Doing recovery: scanned up to log sequence number {}, recovered up to {}", end_lsn, recv.m_recovered_lsn
      ));
    }

    if (recv.m_found_corrupt_log && sys.m_config.m_recv.m_force_recovery == IB_RECOVERY_DEFAULT) {
      log.release();

      log_err("The log is corrupt: set force_recovery to recover up to the corruption.");

      return DB_ERROR;
    }
  }

  /* The log continues after the last complete record group. */
  log.m_lsn = recv.m_recovered_lsn;

  if (recv.m_needed_recovery) {
    sys.m_srv->m_start_lsn = recv.m_recovered_lsn;

    /* Apply all the records and write the pages: the WAL starts at the next
    block boundary with a new checkpoint, recovery never reads this log again. */
    recv_apply_log_recs(sys, true);
  }

  mutex_enter(&recv.m_mutex);

  recv.m_apply_log_recs = true;

  mutex_exit(&recv.m_mutex);

  log.release();

  /* The database is now ready to start almost normal processing of user
  transactions: transaction rollbacks and the application of the log
  records in the hash table can be run in background. */

  return DB_SUCCESS;
}

void recv_recovery_from_checkpoint_finish(System &sys) noexcept {
  /* Apply the hashed log records to the respective file pages */

  if (sys.m_config.m_recv.m_force_recovery < IB_RECOVERY_NO_LOG_REDO) {

    recv_apply_log_recs(sys, false);
  }

  if (sys.m_recv->m_found_corrupt_log) {

    log_warn(
      "WARNING: the log may have been corrupt and it"
      " is possible that the log scan or parsing did not proceed"
      " far enough in recovery. Please run CHECK TABLE"
      " on your InnoDB tables to check that they are ok!"
      " It may be safest to recover your InnoDB database from"
      " a backup!"
    );
  }

  /* Free the resources of the recovery system */

  sys.m_recv->m_recovery_on = false;

  sys.m_recv->close();

  /* Free up the flush_rbt. */
  sys.m_buf_pool->m_flusher->free_flush_list();

  /* Roll back any recovered data dictionary transactions, so
  that the data dictionary tables will be free of any locks.
  The data dictionary latch should guarantee that there is at
  most one data dictionary transaction active at a time. */
  trx_rollback_or_clean_recovered(sys, false);
}

void recv_recovery_rollback_active(System &sys) noexcept {

  (void)sys.m_dict->m_ddl.drop_all_temp_indexes(IbRecovery(sys.m_config.m_recv.m_force_recovery));
  (void)sys.m_dict->m_ddl.drop_all_temp_tables(IbRecovery(sys.m_config.m_recv.m_force_recovery));

  if (sys.m_config.m_recv.m_force_recovery < IB_RECOVERY_NO_TRX_UNDO) {
    /* Rollback the uncommitted transactions which have no user
    session */

    sys.m_srv->create_thread(trx_rollback_or_clean_all_recovered, &sys);
  }
}

std::string to_string(RecvAddrState s) noexcept {
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
      log_fatal(std::format("Unknown recv_addr_state: {}", (int)s));
      return "UNKNOWN";
  }
}

std::string LogRecordData::to_string() const noexcept {
  return std::format("FIXME");
}

std::string LogRecord::to_string() const noexcept {
  return std::format(
    "m_type: {}, : {}, m_start_lsn: {}, m_end_lsn: {}, data: {}",
    mlog_type_str(m_type),
    m_len,
    m_start_lsn,
    m_end_lsn,
    m_data->to_string()
  );
}

std::string PageLogRecords::to_string() const noexcept {
  std::string str{};

  for (auto lr = m_rec_head; lr != nullptr; lr = lr->m_next) {
    str += lr->to_string();
    str += ", ";
  }

  return str;
}

std::string RecvSys::to_string() const noexcept {
  std::string str = std::format(
    "m_parse_start_lsn: {}, m_scanned_lsn: {}, m_recovered_lsn: {},"
    " m_recovered_offset: {}, m_len: {}, m_apply_log_recs: {}, m_apply_batch_on:"
    " {}, m_found_corrupt_log: {}, m_n_log_records: {}",
    m_parse_start_lsn,
    m_scanned_lsn,
    m_recovered_lsn,
    m_recovered_offset,
    m_len,
    m_apply_log_recs,
    m_apply_batch_on,
    m_found_corrupt_log,
    m_n_log_records
  );

  return str + ::to_string(m_log_records);
}
