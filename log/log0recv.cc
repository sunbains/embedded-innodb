/****************************************************************************
Copyright (c) 1997, 2010, Innobase Oy. All Rights Reserved.

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
constexpr ulint RECV_DATA_BLOCK_SIZE = MEM_MAX_ALLOC_IN_BUF - sizeof(recv_data_t);

/** Read-ahead area in applying log records to file pages */
constexpr ulint RECV_READ_AHEAD_AREA = 32;

/** The recovery system */
recv_sys_t *recv_sys = nullptr;

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

/** true when the redo log is being backed up */
#define recv_is_making_a_backup false

/** true when recovering from a backed up redo log file */
#define recv_is_from_backup false

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
uint64_t recv_max_page_lsn;

/* prototypes */

/** Initialize crash recovery environment. Can be called iff
recv_needed_recovery == false. */
static void recv_start_crash_recovery(ib_recovery_t recovery); /*!< in: recovery flag */

void recv_sys_var_init() {
  recv_sys = nullptr;

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

void recv_sys_create() {
  if (recv_sys != nullptr) {

    return;
  }

  recv_sys = static_cast<recv_sys_t *>(mem_alloc(sizeof(*recv_sys)));

  mutex_create(&recv_sys->mutex, SYNC_RECV);

  recv_sys->heap = nullptr;
  recv_sys->addr_hash = nullptr;
}

void recv_sys_close(void) {
  if (recv_sys != nullptr) {
    mutex_free(&recv_sys->mutex);
  }
}

void recv_sys_mem_free() {
  if (recv_sys != nullptr) {
    if (recv_sys->addr_hash != nullptr) {
      hash_table_free(recv_sys->addr_hash);
    }

    if (recv_sys->heap != nullptr) {
      mem_heap_free(recv_sys->heap);
    }

    if (recv_sys->buf != nullptr) {
      ut_free(recv_sys->buf);
    }

    if (recv_sys->last_block_buf_start != nullptr) {
      mem_free(recv_sys->last_block_buf_start);
    }

    mem_free(recv_sys);
    recv_sys = nullptr;
  }
}

void recv_sys_init(ulint available_memory) {
  if (recv_sys->heap != nullptr) {

    return;
  }

  mutex_enter(&(recv_sys->mutex));

  recv_sys->heap = mem_heap_create_in_buffer(256);

  /* Set appropriate value of recv_n_pool_free_frames. */
  if (buf_pool_get_curr_size() >= (10 * 1024 * 1024)) {
    /* Buffer pool of size greater than 10 MB. */
    recv_n_pool_free_frames = 512;
  }

  recv_sys->buf = static_cast<byte *>(ut_malloc(RECV_PARSING_BUF_SIZE));
  recv_sys->len = 0;
  recv_sys->recovered_offset = 0;

  recv_sys->addr_hash = hash_create(available_memory / 64);
  recv_sys->n_addrs = 0;

  recv_sys->apply_log_recs = false;
  recv_sys->apply_batch_on = false;

  recv_sys->last_block_buf_start = static_cast<byte *>(mem_alloc(2 * IB_FILE_BLOCK_SIZE));

  recv_sys->last_block = static_cast<byte *>(ut_align(recv_sys->last_block_buf_start, IB_FILE_BLOCK_SIZE));
  recv_sys->found_corrupt_log = false;

  recv_max_page_lsn = 0;

  mutex_exit(&(recv_sys->mutex));
}

/** Empties the hash table when it has been fully processed. */
static void recv_sys_empty_hash(void) {
  ut_ad(mutex_own(&(recv_sys->mutex)));

  if (recv_sys->n_addrs != 0) {
    ib_logger(
      ib_stream,
      "Error: %lu pages with log records"
      " were left unprocessed!\n"
      "Maximum page number with"
      " log records on it %lu\n",
      (ulong)recv_sys->n_addrs,
      (ulong)recv_max_parsed_page_no
    );
    ut_error;
  }

  hash_table_free(recv_sys->addr_hash);
  mem_heap_empty(recv_sys->heap);

  recv_sys->addr_hash = hash_create(buf_pool_get_curr_size() / 256);
}

#ifndef UNIV_LOG_DEBUG
/** Frees the recovery system. */
static void recv_sys_debug_free(void) {
  mutex_enter(&(recv_sys->mutex));

  hash_table_free(recv_sys->addr_hash);
  mem_heap_free(recv_sys->heap);
  ut_free(recv_sys->buf);
  mem_free(recv_sys->last_block_buf_start);

  recv_sys->buf = nullptr;
  recv_sys->heap = nullptr;
  recv_sys->addr_hash = nullptr;
  recv_sys->last_block_buf_start = nullptr;

  mutex_exit(&(recv_sys->mutex));

  /* Free up the flush_rbt. */
  buf_flush_free_flush_rbt();
}
#endif /* UNIV_LOG_DEBUG */

/** Truncates possible corrupted or extra records from a log group. */
static void recv_truncate_group(
  log_group_t *group,      /*!< in: log group */
  uint64_t recovered_lsn,  /*!< in: recovery succeeded up to
                                                this lsn */
  uint64_t limit_lsn,      /*!< in: this was the limit for
                                                recovery */
  uint64_t checkpoint_lsn, /*!< in: recovery was started
                                                from this checkpoint */
  uint64_t archived_lsn
) /*!< in: the log has been archived
                                                up to this lsn */
{
  uint64_t start_lsn;
  uint64_t end_lsn;
  uint64_t finish_lsn1;
  uint64_t finish_lsn2;
  uint64_t finish_lsn;
  ulint len;
  ulint i;

  if (archived_lsn == IB_UINT64_T_MAX) {
    /* Checkpoint was taken in the NOARCHIVELOG mode */
    archived_lsn = checkpoint_lsn;
  }

  finish_lsn1 = ut_uint64_align_down(archived_lsn, IB_FILE_BLOCK_SIZE) + log_group_get_capacity(group);

  finish_lsn2 = ut_uint64_align_up(recovered_lsn, IB_FILE_BLOCK_SIZE) + recv_sys->last_log_buf_size;

  if (limit_lsn != IB_UINT64_T_MAX) {
    /* We do not know how far we should erase log records: erase
    as much as possible */

    finish_lsn = finish_lsn1;
  } else {
    /* It is enough to erase the length of the log buffer */
    finish_lsn = finish_lsn1 < finish_lsn2 ? finish_lsn1 : finish_lsn2;
  }

  ut_a(RECV_SCAN_SIZE <= log_sys->buf_size);

  /* Write the log buffer full of zeros */
  for (i = 0; i < RECV_SCAN_SIZE; i++) {

    *(log_sys->buf + i) = '\0';
  }

  start_lsn = ut_uint64_align_down(recovered_lsn, IB_FILE_BLOCK_SIZE);

  if (start_lsn != recovered_lsn) {
    /* Copy the last incomplete log block to the log buffer and
    edit its data length: */

    memcpy(log_sys->buf, recv_sys->last_block, IB_FILE_BLOCK_SIZE);
    log_block_set_data_len(log_sys->buf, (ulint)(recovered_lsn - start_lsn));
  }

  if (start_lsn >= finish_lsn) {

    return;
  }

  for (;;) {
    end_lsn = start_lsn + RECV_SCAN_SIZE;

    if (end_lsn > finish_lsn) {

      end_lsn = finish_lsn;
    }

    len = (ulint)(end_lsn - start_lsn);

    log_group_write_buf(group, log_sys->buf, len, start_lsn, 0);
    if (end_lsn >= finish_lsn) {

      return;
    }

    /* Write the log buffer full of zeros */
    for (i = 0; i < RECV_SCAN_SIZE; i++) {

      *(log_sys->buf + i) = '\0';
    }

    start_lsn = end_lsn;
  }
}

/** Copies the log segment between group->recovered_lsn and recovered_lsn from
the most up-to-date log group to group, so that it contains the latest log data.
*/
static void recv_copy_group(
  log_group_t *up_to_date_group, /*!< in: the most up-to-date log
                                               group */
  log_group_t *group,            /*!< in: copy to this log
                                               group */
  uint64_t recovered_lsn
) /*!< in: recovery succeeded up
                                                  to this lsn */
{
  uint64_t start_lsn;
  uint64_t end_lsn;
  ulint len;

  if (group->scanned_lsn >= recovered_lsn) {

    return;
  }

  ut_a(RECV_SCAN_SIZE <= log_sys->buf_size);

  start_lsn = ut_uint64_align_down(group->scanned_lsn, IB_FILE_BLOCK_SIZE);
  for (;;) {
    end_lsn = start_lsn + RECV_SCAN_SIZE;

    if (end_lsn > recovered_lsn) {
      end_lsn = ut_uint64_align_up(recovered_lsn, IB_FILE_BLOCK_SIZE);
    }

    log_group_read_log_seg(LOG_RECOVER, log_sys->buf, up_to_date_group, start_lsn, end_lsn);

    len = (ulint)(end_lsn - start_lsn);

    log_group_write_buf(group, log_sys->buf, len, start_lsn, 0);

    if (end_lsn >= recovered_lsn) {

      return;
    }

    start_lsn = end_lsn;
  }
}

/** Copies a log segment from the most up-to-date log group to the other log
groups, so that they all contain the latest log data. Also writes the info
about the latest checkpoint to the groups, and inits the fields in the group
memory structs to up-to-date values. */
static void recv_synchronize_groups(log_group_t *up_to_date_group) /*!< in: the most
                                                       up-to-date log group */
{
  log_group_t *group;
  uint64_t start_lsn;
  uint64_t end_lsn;
  uint64_t recovered_lsn;

  recovered_lsn = recv_sys->recovered_lsn;

  /* Read the last recovered log block to the recovery system buffer:
  the block is always incomplete */

  start_lsn = ut_uint64_align_down(recovered_lsn, IB_FILE_BLOCK_SIZE);
  end_lsn = ut_uint64_align_up(recovered_lsn, IB_FILE_BLOCK_SIZE);

  ut_a(start_lsn != end_lsn);

  log_group_read_log_seg(LOG_RECOVER, recv_sys->last_block, up_to_date_group, start_lsn, end_lsn);

  group = UT_LIST_GET_FIRST(log_sys->log_groups);

  while (group) {
    if (group != up_to_date_group) {

      /* Copy log data if needed */

      recv_copy_group(group, up_to_date_group, recovered_lsn);
    }

    /* Update the fields in the group struct to correspond to
    recovered_lsn */

    log_group_set_fields(group, recovered_lsn);

    group = UT_LIST_GET_NEXT(log_groups, group);
  }

  /* Copy the checkpoint info to the groups; remember that we have
  incremented checkpoint_no by one, and the info will not be written
  over the max checkpoint info, thus making the preservation of max
  checkpoint info on disk certain */

  log_groups_write_checkpoint_info();

  mutex_exit(&log_sys->mutex);

  /* Wait for the checkpoint write to complete */
  rw_lock_s_lock(&(log_sys->checkpoint_lock));
  rw_lock_s_unlock(&(log_sys->checkpoint_lock));

  mutex_enter(&log_sys->mutex);
}

/** Checks the consistency of the checkpoint info
@return	true if ok */
static bool recv_check_cp_is_consistent(const byte *buf) /*!< in: buffer containing checkpoint info */
{
  ulint fold;

  fold = ut_fold_binary(buf, LOG_CHECKPOINT_CHECKSUM_1);

  if ((fold & 0xFFFFFFFFUL) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1)) {
    return false;
  }

  fold = ut_fold_binary(buf + LOG_CHECKPOINT_LSN, LOG_CHECKPOINT_CHECKSUM_2 - LOG_CHECKPOINT_LSN);

  if ((fold & 0xFFFFFFFFUL) != mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_2)) {
    return false;
  }

  return true;
}

/** Looks for the maximum consistent checkpoint from the log groups.
@return	error code or DB_SUCCESS */
static db_err recv_find_max_checkpoint(
  log_group_t **max_group, /*!< out: max group */
  ulint *max_field
) /*!< out: LOG_CHECKPOINT_1 or
                                                  LOG_CHECKPOINT_2 */
{
  log_group_t *group;
  uint64_t max_no;
  uint64_t checkpoint_no;
  ulint field;
  byte *buf;

  group = UT_LIST_GET_FIRST(log_sys->log_groups);

  max_no = 0;
  *max_group = nullptr;
  *max_field = 0;

  buf = log_sys->checkpoint_buf;

  while (group) {
    group->state = LOG_GROUP_CORRUPTED;

    for (field = LOG_CHECKPOINT_1; field <= LOG_CHECKPOINT_2; field += LOG_CHECKPOINT_2 - LOG_CHECKPOINT_1) {

      log_group_read_checkpoint_info(group, field);

      if (!recv_check_cp_is_consistent(buf)) {
#ifdef UNIV_DEBUG
        if (log_debug_writes) {
          ib_logger(
            ib_stream,
            "Checkpoint in group"
            " %lu at %lu invalid, %lu\n",
            (ulong)group->id,
            (ulong)field,
            (ulong)mach_read_from_4(buf + LOG_CHECKPOINT_CHECKSUM_1)
          );
        }
#endif /* UNIV_DEBUG */
        goto not_consistent;
      }

      group->state = LOG_GROUP_OK;

      group->lsn = mach_read_from_8(buf + LOG_CHECKPOINT_LSN);
      group->lsn_offset = mach_read_from_4(buf + LOG_CHECKPOINT_OFFSET);
      checkpoint_no = mach_read_from_8(buf + LOG_CHECKPOINT_NO);

#ifdef UNIV_DEBUG
      if (log_debug_writes) {
        ib_logger(
          ib_stream,
          "Checkpoint number %lu"
          " found in group %lu\n",
          (ulong)checkpoint_no,
          (ulong)group->id
        );
      }
#endif /* UNIV_DEBUG */

      if (checkpoint_no >= max_no) {
        *max_group = group;
        *max_field = field;
        max_no = checkpoint_no;
      }

    not_consistent:;
    }

    group = UT_LIST_GET_NEXT(log_groups, group);
  }

  if (*max_group == nullptr) {

    ib_logger(
      ib_stream,
      "No valid checkpoint found.\n"
      "If this error appears when you are"
      " creating an InnoDB database,\n"
      "the problem may be that during"
      " an earlier attempt you managed\n"
      "to create the InnoDB data files,"
      " but log file creation failed.\n"
      "If that is the case, please refer to\n"
      "the InnoDB website for details\n"
    );
    return DB_ERROR;
  }

  return DB_SUCCESS;
}

/** Checks the 4-byte checksum to the trailer checksum field of a log
block.  We also accept a log block in the old format before
InnoDB-3.23.52 where the checksum field contains the log block number.
@return true if ok, or if the log block may be in the format of InnoDB
version predating 3.23.52 */
static bool log_block_checksum_is_ok_or_old_format(const byte *block) /*!< in: pointer to a log block */
{
#ifdef UNIV_LOG_DEBUG
  return true;
#endif /* UNIV_LOG_DEBUG */
  if (log_block_calc_checksum(block) == log_block_get_checksum(block)) {

    return true;
  }

  if (log_block_get_hdr_no(block) == log_block_get_checksum(block)) {

    /* We assume the log block is in the format of
    InnoDB version < 3.23.52 and the block is ok */
    return true;
  }

  return false;
}

/** Tries to parse a single log record body and also applies it to a page if
specified. File ops are parsed, but not applied in this function.
@return	log record end, nullptr if not a complete record */
static byte *recv_parse_or_apply_log_rec_body(
  byte type,          /*!< in: type */
  byte *ptr,          /*!< in: pointer to a buffer */
  byte *end_ptr,      /*!< in: pointer to the buffer end */
  buf_block_t *block, /*!< in/out: buffer block or nullptr; if
                        not nullptr, then the log record is
                        applied to the page, and the log
                        record should be complete then */
  mtr_t *mtr
) /*!< in: mtr or nullptr; should be non-nullptr
                        if and only if block is non-nullptr */
{
  dict_index_t *index = nullptr;
  page_t *page;
#ifdef UNIV_DEBUG
  ulint page_type;
#endif /* UNIV_DEBUG */

  ut_ad(!block == !mtr);

  if (block) {
    page = block->frame;
    ut_d(page_type = fil_page_get_type(page));
  } else {
    page = nullptr;
    ut_d(page_type = FIL_PAGE_TYPE_ALLOCATED);
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
      if (page && page_type == FIL_PAGE_TYPE_ALLOCATED && end_ptr >= ptr + 2) {
        /* It is OK to set FIL_PAGE_TYPE and certain
      list node fields on an empty page.  Any other
      write is not OK. */

        /* NOTE: There may be bogus assertion failures for
      dict_hdr_create(), trx_rseg_header_create(),
      trx_sys_create_doublewrite_buf(), and
      trx_sysf_create().
      These are only called during database creation. */
        ulint offs = mach_read_from_2(ptr);

        switch (type) {
          default:
            ut_error;
          case MLOG_2BYTES:
            /* Note that this can fail when the
        redo log been written with something
        older than InnoDB Plugin 1.0.4. */
            ut_ad(
              offs == FIL_PAGE_TYPE || offs == PAGE_BTR_SEG_LEAF + PAGE_HEADER + FSEG_HDR_OFFSET ||
              offs == PAGE_BTR_SEG_TOP + PAGE_HEADER + FSEG_HDR_OFFSET
            );
            break;
          case MLOG_4BYTES:
            /* Note that this can fail when the
        redo log been written with something
        older than InnoDB Plugin 1.0.4. */
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
    case MLOG_COMP_REC_INSERT:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, type == MLOG_COMP_REC_INSERT, &index))) {
        ut_a(!page || (bool)!!page_is_comp(page) == dict_table_is_comp(index->table));
        ptr = page_cur_parse_insert_rec(false, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_REC_CLUST_DELETE_MARK:
    case MLOG_COMP_REC_CLUST_DELETE_MARK:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, type == MLOG_COMP_REC_CLUST_DELETE_MARK, &index))) {
        ut_a(!page || (bool)!!page_is_comp(page) == dict_table_is_comp(index->table));
        ptr = btr_cur_parse_del_mark_set_clust_rec(ptr, end_ptr, page, index);
      }
      break;
    case MLOG_COMP_REC_SEC_DELETE_MARK:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);
      /* This log record type is obsolete, but we process it for
    backward compatibility with v5.0.3 and v5.0.4. */
      ut_a(!page || page_is_comp(page));
      ptr = mlog_parse_index(ptr, end_ptr, true, &index);
      if (!ptr) {
        break;
      }
      /* Fall through */
    case MLOG_REC_SEC_DELETE_MARK:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);
      ptr = btr_cur_parse_del_mark_set_sec_rec(ptr, end_ptr, page);
      break;
    case MLOG_REC_UPDATE_IN_PLACE:
    case MLOG_COMP_REC_UPDATE_IN_PLACE:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, type == MLOG_COMP_REC_UPDATE_IN_PLACE, &index))) {
        ut_a(!page || (bool)!!page_is_comp(page) == dict_table_is_comp(index->table));
        ptr = btr_cur_parse_update_in_place(ptr, end_ptr, page, index);
      }
      break;
    case MLOG_LIST_END_DELETE:
    case MLOG_COMP_LIST_END_DELETE:
    case MLOG_LIST_START_DELETE:
    case MLOG_COMP_LIST_START_DELETE:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, type == MLOG_COMP_LIST_END_DELETE || type == MLOG_COMP_LIST_START_DELETE, &index))) {
        ut_a(!page || (bool)!!page_is_comp(page) == dict_table_is_comp(index->table));
        ptr = page_parse_delete_rec_list(type, ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_LIST_END_COPY_CREATED:
    case MLOG_COMP_LIST_END_COPY_CREATED:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, type == MLOG_COMP_LIST_END_COPY_CREATED, &index))) {
        ut_a(!page || (bool)!!page_is_comp(page) == dict_table_is_comp(index->table));
        ptr = page_parse_copy_rec_list_to_created_page(ptr, end_ptr, block, index, mtr);
      }
      break;
    case MLOG_PAGE_REORGANIZE:
    case MLOG_COMP_PAGE_REORGANIZE:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, type == MLOG_COMP_PAGE_REORGANIZE, &index))) {
        ut_a(!page || (bool)!!page_is_comp(page) == dict_table_is_comp(index->table));
        ptr = btr_parse_page_reorganize(ptr, end_ptr, index, block, mtr);
      }
      break;
    case MLOG_PAGE_CREATE:
    case MLOG_COMP_PAGE_CREATE:
      /* Allow anything in page_type when creating a page. */
      ptr = page_parse_create(ptr, end_ptr, type == MLOG_COMP_PAGE_CREATE, block, mtr);
      break;
    case MLOG_UNDO_INSERT:
      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
      ptr = trx_undo_parse_add_undo_rec(ptr, end_ptr, page);
      break;
    case MLOG_UNDO_ERASE_END:
      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
      ptr = trx_undo_parse_erase_page_end(ptr, end_ptr, page, mtr);
      break;
    case MLOG_UNDO_INIT:
      /* Allow anything in page_type when creating a page. */
      ptr = trx_undo_parse_page_init(ptr, end_ptr, page, mtr);
      break;
    case MLOG_UNDO_HDR_DISCARD:
      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
      ptr = trx_undo_parse_discard_latest(ptr, end_ptr, page, mtr);
      break;
    case MLOG_UNDO_HDR_CREATE:
    case MLOG_UNDO_HDR_REUSE:
      ut_ad(!page || page_type == FIL_PAGE_UNDO_LOG);
      ptr = trx_undo_parse_page_header(type, ptr, end_ptr, page, mtr);
      break;
    case MLOG_REC_MIN_MARK:
    case MLOG_COMP_REC_MIN_MARK:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);
      ut_a(type == MLOG_COMP_REC_MIN_MARK);
      ptr = btr_parse_set_min_rec_mark(ptr, end_ptr, type == MLOG_COMP_REC_MIN_MARK, page, mtr);
      break;
    case MLOG_REC_DELETE:
    case MLOG_COMP_REC_DELETE:
      ut_ad(!page || page_type == FIL_PAGE_INDEX);

      if (nullptr != (ptr = mlog_parse_index(ptr, end_ptr, type == MLOG_COMP_REC_DELETE, &index))) {
        ut_a(!page || (bool)!!page_is_comp(page) == dict_table_is_comp(index->table));
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
    case MLOG_FILE_CREATE2:
      ptr = fil_op_log_parse_or_replay(ptr, end_ptr, type, 0, 0);
      break;
    default:
      ptr = nullptr;
      recv_sys->found_corrupt_log = true;
  }

  if (index) {
    dict_table_t *table = index->table;

    dict_mem_index_free(index);
    dict_mem_table_free(table);
  }

  return ptr;
}

/** Calculates the fold value of a page file address: used in inserting or
searching for a log record in the hash table.
@return	folded value */
inline ulint recv_fold(
  ulint space, /*!< in: space */
  ulint page_no
) /*!< in: page number */
{
  return ut_fold_ulint_pair(space, page_no);
}

/** Calculates the hash value of a page file address: used in inserting or
searching for a log record in the hash table.
@return	folded value */
inline ulint recv_hash(
  ulint space, /*!< in: space */
  ulint page_no
) /*!< in: page number */
{
  return hash_calc_hash(recv_fold(space, page_no), recv_sys->addr_hash);
}

/** Gets the hashed file address struct for a page.
@return	file address struct, nullptr if not found from the hash table */
static recv_addr_t *recv_get_fil_addr_struct(
  ulint space, /*!< in: space id */
  ulint page_no
) /*!< in: page number */
{
  auto recv_addr = static_cast<recv_addr_t *>(HASH_GET_FIRST(recv_sys->addr_hash, recv_hash(space, page_no)));

  while (recv_addr != nullptr) {
    if ((recv_addr->space == space) && (recv_addr->page_no == page_no)) {

      break;
    }

    recv_addr = static_cast<recv_addr_t *>(HASH_GET_NEXT(addr_hash, recv_addr));
  }

  return recv_addr;
}

/** Adds a new log record to the hash table of log records. */
static void recv_add_to_hash_table(
  byte type,          /*!< in: log record type */
  ulint space,        /*!< in: space id */
  ulint page_no,      /*!< in: page number */
  byte *body,         /*!< in: log record body */
  byte *rec_end,      /*!< in: log record end */
  uint64_t start_lsn, /*!< in: start lsn of the mtr */
  uint64_t end_lsn
) /*!< in: end lsn of the mtr */
{
  ulint len;
  recv_data_t *recv_data;
  recv_data_t **prev_field;
  recv_addr_t *recv_addr;

  if (fil_tablespace_deleted_or_being_deleted_in_mem(space, -1)) {
    /* The tablespace does not exist any more: do not store the
    log record */

    return;
  }

  len = rec_end - body;

  auto recv = reinterpret_cast<recv_t *>(mem_heap_alloc(recv_sys->heap, sizeof(recv_t)));

  recv->type = type;
  recv->len = rec_end - body;
  recv->start_lsn = start_lsn;
  recv->end_lsn = end_lsn;

  recv_addr = recv_get_fil_addr_struct(space, page_no);

  if (recv_addr == nullptr) {
    recv_addr = reinterpret_cast<recv_addr_t *>(mem_heap_alloc(recv_sys->heap, sizeof(recv_addr_t)));

    recv_addr->space = space;
    recv_addr->page_no = page_no;
    recv_addr->state = RECV_NOT_PROCESSED;

    UT_LIST_INIT(recv_addr->rec_list);

    HASH_INSERT(recv_addr_t, addr_hash, recv_sys->addr_hash, recv_fold(space, page_no), recv_addr);
    recv_sys->n_addrs++;
  }

  UT_LIST_ADD_LAST(recv_addr->rec_list, recv);

  prev_field = &(recv->data);

  /* Store the log record body in chunks of less than UNIV_PAGE_SIZE:
  recv_sys->heap grows into the buffer pool, and bigger chunks could not
  be allocated */

  while (rec_end > body) {

    len = rec_end - body;

    if (len > RECV_DATA_BLOCK_SIZE) {
      len = RECV_DATA_BLOCK_SIZE;
    }

    recv_data = reinterpret_cast<recv_data_t *>(mem_heap_alloc(recv_sys->heap, sizeof(recv_data_t) + len));

    *prev_field = recv_data;

    memcpy(recv_data + 1, body, len);

    prev_field = &(recv_data->next);

    body += len;
  }

  *prev_field = nullptr;
}

/** Copies the log record body from recv to buf. */
static void recv_data_copy_to_buf(
  byte *buf, /*!< in: buffer of length at least recv->len */
  recv_t *recv
) /*!< in: log record */
{
  recv_data_t *recv_data;
  ulint part_len;
  ulint len;

  len = recv->len;
  recv_data = recv->data;

  while (len > 0) {
    if (len > RECV_DATA_BLOCK_SIZE) {
      part_len = RECV_DATA_BLOCK_SIZE;
    } else {
      part_len = len;
    }

    memcpy(buf, ((byte *)recv_data) + sizeof(recv_data_t), part_len);
    buf += part_len;
    len -= part_len;

    recv_data = recv_data->next;
  }
}

void recv_recover_page_func(bool just_read_in, buf_block_t *block) {
  page_t *page;
  recv_addr_t *recv_addr;
  recv_t *recv;
  byte *buf;
  uint64_t start_lsn;
  uint64_t end_lsn;
  uint64_t page_lsn;
  uint64_t page_newest_lsn;
  bool modification_to_page;
  bool success;
  mtr_t mtr;

  mutex_enter(&(recv_sys->mutex));

  if (recv_sys->apply_log_recs == false) {

    /* Log records should not be applied now */

    mutex_exit(&(recv_sys->mutex));

    return;
  }

  recv_addr = recv_get_fil_addr_struct(buf_block_get_space(block), buf_block_get_page_no(block));

  if ((recv_addr == nullptr) || (recv_addr->state == RECV_BEING_PROCESSED) || (recv_addr->state == RECV_PROCESSED)) {

    mutex_exit(&(recv_sys->mutex));

    return;
  }

  recv_addr->state = RECV_BEING_PROCESSED;

  mutex_exit(&(recv_sys->mutex));

  mtr_start(&mtr);
  mtr_set_log_mode(&mtr, MTR_LOG_NONE);

  page = block->frame;

  if (just_read_in) {
    /* Move the ownership of the x-latch on the page to
    this OS thread, so that we can acquire a second
    x-latch on it.  This is needed for the operations to
    the page to pass the debug checks. */

    rw_lock_x_lock_move_ownership(&block->lock);
  }

  success = buf_page_get_known_nowait(RW_X_LATCH, block, BUF_KEEP_OLD, __FILE__, __LINE__, &mtr);
  ut_a(success);

  buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

  /* Read the newest modification lsn from the page */
  page_lsn = mach_read_from_8(page + FIL_PAGE_LSN);

  /* It may be that the page has been modified in the buffer
  pool: read the newest modification lsn there */

  page_newest_lsn = buf_page_get_newest_modification(&block->page);

  if (page_newest_lsn) {

    page_lsn = page_newest_lsn;
  }

  modification_to_page = false;
  start_lsn = end_lsn = 0;

  recv = UT_LIST_GET_FIRST(recv_addr->rec_list);

  while (recv) {
    end_lsn = recv->end_lsn;

    if (recv->len > RECV_DATA_BLOCK_SIZE) {
      /* We have to copy the record body to a separate buffer */

      buf = static_cast<byte *>(mem_alloc(recv->len));

      recv_data_copy_to_buf(buf, recv);
    } else {
      buf = ((byte *)(recv->data)) + sizeof(recv_data_t);
    }

    if (recv->type == MLOG_INIT_FILE_PAGE) {
      page_lsn = page_newest_lsn;

      memset(FIL_PAGE_LSN + page, 0, 8);
      memset(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM + page, 0, 8);
    }

    if (recv->start_lsn >= page_lsn) {

      uint64_t end_lsn;

      if (!modification_to_page) {

        modification_to_page = true;
        start_lsn = recv->start_lsn;
      }

#ifdef UNIV_DEBUG
      if (log_debug_writes) {
        ib_logger(
          ib_stream,
          "Applying log rec"
          " type %lu len %lu"
          " to space %lu page no %lu\n",
          (ulong)recv->type,
          (ulong)recv->len,
          (ulong)recv_addr->space,
          (ulong)recv_addr->page_no
        );
      }
#endif /* UNIV_DEBUG */

      recv_parse_or_apply_log_rec_body(recv->type, buf, buf + recv->len, block, &mtr);

      end_lsn = recv->start_lsn + recv->len;
      mach_write_to_8(FIL_PAGE_LSN + page, end_lsn);
      mach_write_to_8(UNIV_PAGE_SIZE - FIL_PAGE_END_LSN_OLD_CHKSUM + page, end_lsn);
    }

    if (recv->len > RECV_DATA_BLOCK_SIZE) {
      mem_free(buf);
    }

    recv = UT_LIST_GET_NEXT(rec_list, recv);
  }

  mutex_enter(&(recv_sys->mutex));

  if (recv_max_page_lsn < page_lsn) {
    recv_max_page_lsn = page_lsn;
  }

  recv_addr->state = RECV_PROCESSED;

  ut_a(recv_sys->n_addrs);
  recv_sys->n_addrs--;

  mutex_exit(&(recv_sys->mutex));

  if (modification_to_page) {
    ut_a(block);

    buf_flush_recv_note_modification(block, start_lsn, end_lsn);
  }

  /* Make sure that committing mtr does not change the modification
  lsn values of page */

  mtr.modifications = false;

  mtr_commit(&mtr);
}

/** Reads in pages which have hashed log records, from an area around a given
page number.
@return	number of pages found */
static ulint recv_read_in_area(
  ulint space, /*!< in: space */
  ulint, ulint page_no
) /*!< in: page number */
{
  recv_addr_t *recv_addr;
  ulint page_nos[RECV_READ_AHEAD_AREA];
  ulint low_limit;
  ulint n;

  low_limit = page_no - (page_no % RECV_READ_AHEAD_AREA);

  n = 0;

  for (page_no = low_limit; page_no < low_limit + RECV_READ_AHEAD_AREA; page_no++) {
    recv_addr = recv_get_fil_addr_struct(space, page_no);

    if (recv_addr && !buf_page_peek(space, page_no)) {

      mutex_enter(&(recv_sys->mutex));

      if (recv_addr->state == RECV_NOT_PROCESSED) {
        recv_addr->state = RECV_BEING_READ;

        page_nos[n] = page_no;

        n++;
      }

      mutex_exit(&(recv_sys->mutex));
    }
  }

  buf_read_recv_pages(false, space, page_nos, n);

  return n;
}

void recv_apply_hashed_log_recs(bool flush_and_free_pages) {
  bool has_printed{};

  for (;;) {
    mutex_enter(&recv_sys->mutex);

    if (!recv_sys->apply_batch_on) {
      break;
    }

    mutex_exit(&recv_sys->mutex);

    os_thread_sleep(100000);
  }

  recv_sys->apply_log_recs = true;
  recv_sys->apply_batch_on = true;

  for (ulint i = 0; i < hash_get_n_cells(recv_sys->addr_hash); i++) {

    auto recv_addr = static_cast<recv_addr_t *>(HASH_GET_FIRST(recv_sys->addr_hash, i));

    while (recv_addr != nullptr) {
      auto space = recv_addr->space;
      auto page_no = recv_addr->page_no;

      if (recv_addr->state == RECV_NOT_PROCESSED) {
        if (!has_printed) {
          ut_print_timestamp(ib_stream);
          ib_logger(ib_stream, "Starting an apply batch of log records to the database...");
          ib_logger(ib_stream, "Progress in percents: ");
          has_printed = true;
        }

        mutex_exit(&(recv_sys->mutex));

        if (buf_page_peek(space, page_no)) {

          mtr_t mtr;

          mtr_start(&mtr);

          auto block = buf_page_get(space, 0, page_no, RW_X_LATCH, &mtr);
          buf_block_dbg_add_level(block, SYNC_NO_ORDER_CHECK);

          recv_recover_page(false, block);

          mtr_commit(&mtr);

        } else {
          recv_read_in_area(space, 0, page_no);
        }

        mutex_enter(&recv_sys->mutex);
      }

      recv_addr = static_cast<recv_addr_t *>(HASH_GET_NEXT(addr_hash, recv_addr));
    }

    if (has_printed && (i * 100) / hash_get_n_cells(recv_sys->addr_hash) != ((i + 1) * 100) / hash_get_n_cells(recv_sys->addr_hash)) {

      ib_logger(ib_stream, "%lu ", (ulong)((i * 100) / hash_get_n_cells(recv_sys->addr_hash)));
    }
  }

  /* Wait until all the pages have been processed */

  while (recv_sys->n_addrs != 0) {

    mutex_exit(&recv_sys->mutex);

    os_thread_sleep(100000);

    mutex_enter(&recv_sys->mutex);
  }

  if (has_printed) {
    ib_logger(ib_stream, "\n");
  }

  if (flush_and_free_pages) {
    /* Flush all the file pages to disk and invalidate them in the buffer pool */
    mutex_exit(&recv_sys->mutex);
    mutex_exit(&log_sys->mutex);

    auto n_pages = buf_flush_batch(BUF_FLUSH_LIST, ULINT_MAX, IB_UINT64_T_MAX);
    ut_a(n_pages != ULINT_UNDEFINED);

    buf_flush_wait_batch_end(BUF_FLUSH_LIST);

    buf_pool_invalidate();

    mutex_enter(&log_sys->mutex);
    mutex_enter(&recv_sys->mutex);
  }

  recv_sys->apply_log_recs = false;
  recv_sys->apply_batch_on = false;

  recv_sys_empty_hash();

  if (has_printed) {
    ib_logger(ib_stream, "Apply batch completed\n");
  }

  mutex_exit(&recv_sys->mutex);
}

/** Tries to parse a single log record and returns its length.
@return	length of the record, or 0 if the record was not complete */
static ulint recv_parse_log_rec(
  byte *ptr,      /*!< in: pointer to a buffer */
  byte *end_ptr,  /*!< in: pointer to the buffer end */
  byte *type,     /*!< out: type */
  ulint *space,   /*!< out: space id */
  ulint *page_no, /*!< out: page number */
  byte **body
) /*!< out: log record body start */
{
  byte *new_ptr;

  *body = nullptr;

  if (ptr == end_ptr) {

    return 0;
  }

  if (*ptr == MLOG_MULTI_REC_END) {

    *type = *ptr;

    return 1;
  }

  if (*ptr == MLOG_DUMMY_RECORD) {
    *type = *ptr;

    *space = ULINT_UNDEFINED - 1; /* For debugging */

    return 1;
  }

  new_ptr = mlog_parse_initial_log_record(ptr, end_ptr, type, space, page_no);
  *body = new_ptr;

  if (unlikely(!new_ptr)) {

    return 0;
  }

#ifdef UNIV_LOG_LSN_DEBUG
  if (*type == MLOG_LSN) {
    uint64_t lsn = uint64_t(*space) << 32 | *page_no;
#ifdef UNIV_LOG_DEBUG
    ut_a(lsn == log_sys->old_lsn);
#else  /* UNIV_LOG_DEBUG */
    ut_a(lsn == recv_sys->recovered_lsn);
#endif /* UNIV_LOG_DEBUG */
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

/** Calculates the new value for lsn when more data is added to the log. */
static uint64_t recv_calc_lsn_on_data_add(
  uint64_t lsn, /*!< in: old lsn */
  uint64_t len
) /*!< in: this many bytes of data is
                     added, log block headers not included */
{
  ulint frag_len;
  ulint lsn_len;

  frag_len = (((ulint)lsn) % IB_FILE_BLOCK_SIZE) - LOG_BLOCK_HDR_SIZE;
  ut_ad(frag_len < IB_FILE_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE);
  lsn_len = (ulint)len;
  lsn_len += (lsn_len + frag_len) / (IB_FILE_BLOCK_SIZE - LOG_BLOCK_HDR_SIZE - LOG_BLOCK_TRL_SIZE) *
             (LOG_BLOCK_HDR_SIZE + LOG_BLOCK_TRL_SIZE);

  return lsn + lsn_len;
}

#ifdef UNIV_LOG_DEBUG
/** Checks that the parser recognizes incomplete initial segments of a log
record as incomplete. */
static void recv_check_incomplete_log_recs(
  byte *ptr, /*!< in: pointer to a complete log record */
  ulint len
) /*!< in: length of the log record */
{
  ulint i;
  byte type;
  ulint space;
  ulint page_no;
  byte *body;

  for (i = 0; i < len; i++) {
    ut_a(0 == recv_parse_log_rec(ptr, ptr + i, &type, &space, &page_no, &body));
  }
}
#endif /* UNIV_LOG_DEBUG */

/** Prints diagnostic info of corrupt log. */
static void recv_report_corrupt_log(
  byte *ptr,   /*!< in: pointer to corrupt log record */
  byte type,   /*!< in: type of the record */
  ulint space, /*!< in: space id, this may also be garbage */
  ulint page_no
) /*!< in: page number, this may also be garbage */
{
  ib_logger(
    ib_stream,
    "############### CORRUPT LOG RECORD FOUND\n"
    "Log record type %lu, space id %lu, page number %lu\n"
    "Log parsing proceeded successfully up to %lu\n"
    "Previous log record type %lu, is multi %lu\n"
    "Recv offset %lu, prev %lu\n",
    (ulong)type,
    (ulong)space,
    (ulong)page_no,
    recv_sys->recovered_lsn,
    (ulong)recv_previous_parsed_rec_type,
    (ulong)recv_previous_parsed_rec_is_multi,
    (ulong)(ptr - recv_sys->buf),
    (ulong)recv_previous_parsed_rec_offset
  );

  if ((ulint)(ptr - recv_sys->buf + 100) > recv_previous_parsed_rec_offset && (ulint)(ptr - recv_sys->buf + 100 - recv_previous_parsed_rec_offset) < 200000) {
    ib_logger(
      ib_stream,
      "Hex dump of corrupt log starting"
      " 100 bytes before the start\n"
      "of the previous log rec,\n"
      "and ending 100 bytes after the start"
      " of the corrupt rec:\n"
    );

    ut_print_buf(
      ib_stream, recv_sys->buf + recv_previous_parsed_rec_offset - 100, ptr - recv_sys->buf + 200 - recv_previous_parsed_rec_offset
    );
    ib_logger(ib_stream, "\n");
  }

  if (!srv_force_recovery) {
    ib_logger(
      ib_stream,
      "Set innodb_force_recovery"
      " to ignore this error.\n"
    );
    ut_error;
  }

  ib_logger(
    ib_stream,
    "WARNING: the log file may have been corrupt and it\n"
    "is possible that the log scan did not proceed\n"
    "far enough in recovery! Please run CHECK TABLE\n"
    "on your InnoDB tables to check that they are ok!\n"
    "If the engine crashes after this recovery, check\n"
    "the InnoDB website for details\n"
    "about forcing recovery.\n"
  );
}

/** Parses log records from a buffer and stores them to a hash table to wait
merging to file pages.
@return	currently always returns false */
static bool recv_parse_log_recs(bool store_to_hash) /*!< in: true if the records should be stored
                         to the hash table; this is set to false if just
                         debug checking is needed */
{
  byte *ptr;
  byte *end_ptr;
  ulint single_rec;
  ulint len;
  ulint total_len;
  uint64_t new_recovered_lsn;
  uint64_t old_lsn;
  byte type;
  ulint space;
  ulint page_no;
  byte *body;

  ut_ad(mutex_own(&log_sys->mutex));
  ut_ad(recv_sys->parse_start_lsn != 0);
loop:
  ptr = recv_sys->buf + recv_sys->recovered_offset;

  end_ptr = recv_sys->buf + recv_sys->len;

  if (ptr == end_ptr) {

    return false;
  }

  single_rec = (ulint)*ptr & MLOG_SINGLE_REC_FLAG;

  if (single_rec || *ptr == MLOG_DUMMY_RECORD) {
    /* The mtr only modified a single page, or this is a file op */

    old_lsn = recv_sys->recovered_lsn;

    /* Try to parse a log record, fetching its type, space id,
    page no, and a pointer to the body of the log record */

    len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);

    if (len == 0 || recv_sys->found_corrupt_log) {
      if (recv_sys->found_corrupt_log) {

        recv_report_corrupt_log(ptr, type, space, page_no);
      }

      return false;
    }

    new_recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);

    if (new_recovered_lsn > recv_sys->scanned_lsn) {
      /* The log record filled a log block, and we require
      that also the next log block should have been scanned
      in */

      return false;
    }

    recv_previous_parsed_rec_type = (ulint)type;
    recv_previous_parsed_rec_offset = recv_sys->recovered_offset;
    recv_previous_parsed_rec_is_multi = 0;

    recv_sys->recovered_offset += len;
    recv_sys->recovered_lsn = new_recovered_lsn;

#ifdef UNIV_DEBUG
    if (log_debug_writes) {
      ib_logger(
        ib_stream,
        "Parsed a single log rec "
        "type %lu len %lu space %lu page no %lu\n",
        (ulong)type,
        (ulong)len,
        (ulong)space,
        (ulong)page_no
      );
    }
#endif /* UNIV_DEBUG */

    if (type == MLOG_DUMMY_RECORD) {
      /* Do nothing */

    } else if (!store_to_hash) {
      /* In debug checking, update a replicate page
      according to the log record, and check that it
      becomes identical with the original page */
#ifdef UNIV_LOG_DEBUG
      recv_check_incomplete_log_recs(ptr, len);
#endif /* UNIV_LOG_DEBUG */

    } else if (type == MLOG_FILE_CREATE || type == MLOG_FILE_CREATE2 || type == MLOG_FILE_RENAME || type == MLOG_FILE_DELETE) {
      ut_a(space);

      /* In normal crash recovery we do not try to
      replay file operations */
#ifdef UNIV_LOG_LSN_DEBUG
    } else if (type == MLOG_LSN) {
      /* Do not add these records to the hash table.
      The page number and space id fields are misused
      for something else. */
#endif /* UNIV_LOG_LSN_DEBUG */
    } else {
      recv_add_to_hash_table(type, space, page_no, body, ptr + len, old_lsn, recv_sys->recovered_lsn);
    }
  } else {
    /* Check that all the records associated with the single mtr
    are included within the buffer */

    total_len = 0;

    for (;;) {
      len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);
      if (len == 0 || recv_sys->found_corrupt_log) {

        if (recv_sys->found_corrupt_log) {

          recv_report_corrupt_log(ptr, type, space, page_no);
        }

        return false;
      }

      recv_previous_parsed_rec_type = (ulint)type;
      recv_previous_parsed_rec_offset = recv_sys->recovered_offset + total_len;
      recv_previous_parsed_rec_is_multi = 1;

#ifdef UNIV_LOG_DEBUG
      if ((!store_to_hash) && (type != MLOG_MULTI_REC_END)) {
        recv_check_incomplete_log_recs(ptr, len);
      }
#endif /* UNIV_LOG_DEBUG */

#ifdef UNIV_DEBUG
      if (log_debug_writes) {
        ib_logger(
          ib_stream,
          "Parsed a multi log rec "
          "type %lu len %lu "
          "space %lu page no %lu\n",
          (ulong)type,
          (ulong)len,
          (ulong)space,
          (ulong)page_no
        );
      }
#endif /* UNIV_DEBUG */

      total_len += len;

      ptr += len;

      if (type == MLOG_MULTI_REC_END) {

        /* Found the end mark for the records */

        break;
      }
    }

    new_recovered_lsn = recv_calc_lsn_on_data_add(recv_sys->recovered_lsn, total_len);

    if (new_recovered_lsn > recv_sys->scanned_lsn) {
      /* The log record filled a log block, and we require
      that also the next log block should have been scanned
      in */

      return false;
    }

    /* Add all the records to the hash table */

    ptr = recv_sys->buf + recv_sys->recovered_offset;

    for (;;) {
      old_lsn = recv_sys->recovered_lsn;
      len = recv_parse_log_rec(ptr, end_ptr, &type, &space, &page_no, &body);
      if (recv_sys->found_corrupt_log) {

        recv_report_corrupt_log(ptr, type, space, page_no);
      }

      ut_a(len != 0);
      ut_a(0 == ((ulint)*ptr & MLOG_SINGLE_REC_FLAG));

      recv_sys->recovered_offset += len;
      recv_sys->recovered_lsn = recv_calc_lsn_on_data_add(old_lsn, len);
      if (type == MLOG_MULTI_REC_END) {

        /* Found the end mark for the records */

        break;
      }

      if (
        store_to_hash
#ifdef UNIV_LOG_LSN_DEBUG
        && type != MLOG_LSN
#endif /* UNIV_LOG_LSN_DEBUG */
      ) {
        recv_add_to_hash_table(type, space, page_no, body, ptr + len, old_lsn, new_recovered_lsn);
      }

      ptr += len;
    }
  }

  goto loop;
}

/** Adds data from a new log block to the parsing buffer of recv_sys if
recv_sys->parse_start_lsn is non-zero.
@return	true if more data added */
static bool recv_sys_add_to_parsing_buf(
  const byte *log_block, /*!< in: log block */
  uint64_t scanned_lsn
) /*!< in: lsn of how far we were able
                              to find data in this log block */
{
  ulint more_len;
  ulint data_len;
  ulint start_offset;
  ulint end_offset;

  ut_ad(scanned_lsn >= recv_sys->scanned_lsn);

  if (!recv_sys->parse_start_lsn) {
    /* Cannot start parsing yet because no start point for
    it found */

    return false;
  }

  data_len = log_block_get_data_len(log_block);

  if (recv_sys->parse_start_lsn >= scanned_lsn) {

    return false;

  } else if (recv_sys->scanned_lsn >= scanned_lsn) {

    return false;

  } else if (recv_sys->parse_start_lsn > recv_sys->scanned_lsn) {
    more_len = (ulint)(scanned_lsn - recv_sys->parse_start_lsn);
  } else {
    more_len = (ulint)(scanned_lsn - recv_sys->scanned_lsn);
  }

  if (more_len == 0) {

    return false;
  }

  ut_ad(data_len >= more_len);

  start_offset = data_len - more_len;

  if (start_offset < LOG_BLOCK_HDR_SIZE) {
    start_offset = LOG_BLOCK_HDR_SIZE;
  }

  end_offset = data_len;

  if (end_offset > IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE) {
    end_offset = IB_FILE_BLOCK_SIZE - LOG_BLOCK_TRL_SIZE;
  }

  ut_ad(start_offset <= end_offset);

  if (start_offset < end_offset) {
    memcpy(recv_sys->buf + recv_sys->len, log_block + start_offset, end_offset - start_offset);

    recv_sys->len += end_offset - start_offset;

    ut_a(recv_sys->len <= RECV_PARSING_BUF_SIZE);
  }

  return true;
}

/** Moves the parsing buffer data left to the buffer start. */
static void recv_sys_justify_left_parsing_buf() {
  memmove(recv_sys->buf, recv_sys->buf + recv_sys->recovered_offset, recv_sys->len - recv_sys->recovered_offset);

  recv_sys->len -= recv_sys->recovered_offset;

  recv_sys->recovered_offset = 0;
}

bool recv_scan_log_recs(
  ib_recovery_t recovery, ulint available_memory, bool store_to_hash, const byte *buf, ulint len, uint64_t start_lsn,
  uint64_t *contiguous_lsn, uint64_t *group_scanned_lsn
) {
  const byte *log_block;
  ulint no;
  uint64_t scanned_lsn;
  bool finished;
  ulint data_len;
  bool more_data;

  ut_ad(start_lsn % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(len % IB_FILE_BLOCK_SIZE == 0);
  ut_ad(len >= IB_FILE_BLOCK_SIZE);

  finished = false;

  log_block = buf;
  scanned_lsn = start_lsn;
  more_data = false;

  do {
    no = log_block_get_hdr_no(log_block);

    if (no != log_block_convert_lsn_to_no(scanned_lsn) || !log_block_checksum_is_ok_or_old_format(log_block)) {

      if (no == log_block_convert_lsn_to_no(scanned_lsn) && !log_block_checksum_is_ok_or_old_format(log_block)) {
        ib_logger(
          ib_stream,
          "Log block no %lu at "
          "lsn %lu has\n"
          "ok header, but checksum "
          "field contains %lu, "
          "should be %lu\n",
          (ulong)no,
          scanned_lsn,
          (ulong)log_block_get_checksum(log_block),
          (ulong)log_block_calc_checksum(log_block)
        );
      }

      /* Garbage or an incompletely written log block */

      finished = true;

      break;
    }

    if (log_block_get_flush_bit(log_block)) {
      /* This block was a start of a log flush operation:
      we know that the previous flush operation must have
      been completed for all log groups before this block
      can have been flushed to any of the groups. Therefore,
      we know that log data is contiguous up to scanned_lsn
      in all non-corrupt log groups. */

      if (scanned_lsn > *contiguous_lsn) {
        *contiguous_lsn = scanned_lsn;
      }
    }

    data_len = log_block_get_data_len(log_block);

    if ((store_to_hash || (data_len == IB_FILE_BLOCK_SIZE)) && scanned_lsn + data_len > recv_sys->scanned_lsn &&
        (recv_sys->scanned_checkpoint_no > 0) && (log_block_get_checkpoint_no(log_block) < recv_sys->scanned_checkpoint_no) &&
        (recv_sys->scanned_checkpoint_no - log_block_get_checkpoint_no(log_block) > 0x80000000UL)) {

      /* Garbage from a log buffer flush which was made
      before the most recent database recovery */

      finished = true;
#ifdef UNIV_LOG_DEBUG
      /* This is not really an error, but currently
      we stop here in the debug version: */

      ut_error;
#endif
      break;
    }

    if (!recv_sys->parse_start_lsn && (log_block_get_first_rec_group(log_block) > 0)) {

      /* We found a point from which to start the parsing
      of log records */

      recv_sys->parse_start_lsn = scanned_lsn + log_block_get_first_rec_group(log_block);
      recv_sys->scanned_lsn = recv_sys->parse_start_lsn;
      recv_sys->recovered_lsn = recv_sys->parse_start_lsn;
    }

    scanned_lsn += data_len;

    if (scanned_lsn > recv_sys->scanned_lsn) {

      /* We have found more entries. If this scan is
      of startup type, we must initiate crash recovery
      environment before parsing these log records. */

      if (!recv_needed_recovery) {

        ib_logger(
          ib_stream,
          "Log scan progressed"
          " past the checkpoint lsn %lu\n",
          recv_sys->scanned_lsn
        );
        recv_start_crash_recovery(recovery);
      }

      /* We were able to find more log data: add it to the
      parsing buffer if parse_start_lsn is already
      non-zero */

      if (recv_sys->len + 4 * IB_FILE_BLOCK_SIZE >= RECV_PARSING_BUF_SIZE) {
        ib_logger(
          ib_stream,
          "Error: log parsing"
          " buffer overflow."
          " Recovery may have failed!\n"
        );

        recv_sys->found_corrupt_log = true;

        if (!srv_force_recovery) {
          ib_logger(
            ib_stream,
            "Set"
            " innodb_force_recovery"
            " to ignore this error.\n"
          );
          ut_error;
        }

      } else if (!recv_sys->found_corrupt_log) {
        more_data = recv_sys_add_to_parsing_buf(log_block, scanned_lsn);
      }

      recv_sys->scanned_lsn = scanned_lsn;
      recv_sys->scanned_checkpoint_no = log_block_get_checkpoint_no(log_block);
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

  if (recv_needed_recovery || (recv_is_from_backup && !recv_is_making_a_backup)) {
    recv_scan_print_counter++;

    if (finished || (recv_scan_print_counter % 80 == 0)) {

      ib_logger(
        ib_stream,
        "Doing recovery: scanned up to"
        " log sequence number %lu\n",
        *group_scanned_lsn
      );
    }
  }

  if (more_data && !recv_sys->found_corrupt_log) {
    /* Try to parse more log records */

    recv_parse_log_recs(store_to_hash);

    if (store_to_hash && mem_heap_get_size(recv_sys->heap) > available_memory) {

      /* Hash table of log records has grown too big: empty it. */

      recv_apply_hashed_log_recs(true);
    }

    if (recv_sys->recovered_offset > RECV_PARSING_BUF_SIZE / 4) {
      /* Move parsing buffer data to the buffer start */

      recv_sys_justify_left_parsing_buf();
    }
  }

  return finished;
}

/** Scans log from a buffer and stores new log data to the parsing buffer.
Parses and hashes the log records if new data found. */
static void recv_group_scan_log_recs(
  ib_recovery_t recovery,   /*!< in: recovery flag */
  log_group_t *group,       /*!< in: log group */
  uint64_t *contiguous_lsn, /*!< in/out: it is known that all log
                                    groups contain contiguous log data up
                                    to this lsn */
  uint64_t *group_scanned_lsn
) /*!< out: scanning succeeded up to
                                  this lsn */
{
  bool finished;
  uint64_t start_lsn;
  uint64_t end_lsn;

  finished = false;

  start_lsn = *contiguous_lsn;

  while (!finished) {
    end_lsn = start_lsn + RECV_SCAN_SIZE;

    log_group_read_log_seg(LOG_RECOVER, log_sys->buf, group, start_lsn, end_lsn);

    finished = recv_scan_log_recs(
      recovery,
      (buf_pool->curr_size - recv_n_pool_free_frames) * UNIV_PAGE_SIZE,
      true,
      log_sys->buf,
      RECV_SCAN_SIZE,
      start_lsn,
      contiguous_lsn,
      group_scanned_lsn
    );
    start_lsn = end_lsn;
  }

#ifdef UNIV_DEBUG
  if (log_debug_writes) {
    ib_logger(
      ib_stream,
      "Scanned group %lu up to"
      " log sequence number %lu\n",
      (ulong)group->id,
      *group_scanned_lsn
    );
  }
#endif /* UNIV_DEBUG */
}

/** Initialize crash recovery environment. Can be called iff
recv_needed_recovery == false. */
static void recv_start_crash_recovery(ib_recovery_t recovery) /*!< in: recovery flag */
{
  ut_a(!recv_needed_recovery);

  recv_needed_recovery = true;

  ut_print_timestamp(ib_stream);

  ib_logger(
    ib_stream,
    " Database was not shut down normally!\n"
    "Starting crash recovery.\n"
  );

  ib_logger(
    ib_stream,
    "Reading tablespace information"
    " from the .ibd files...\n"
  );

  fil_load_single_table_tablespaces(recovery);

  /* If we are using the doublewrite method, we will
  check if there are half-written pages in data files,
  and restore them from the doublewrite buffer if
  possible */

  if (recovery < IB_RECOVERY_NO_LOG_REDO) {

    ib_logger(
      ib_stream,
      "Restoring possible half-written data "
      "pages from the doublewrite\n"
      "buffer...\n"
    );
    trx_sys_doublewrite_init_or_restore_pages(true);
  }
}

/** Recover form ibbackup log file. */
static void recv_recover_from_ibbackup(log_group_t *max_cp_group) /*!< in/out: log group that contains the
                               maximum consistent checkpoint */
{
  byte log_hdr_buf[LOG_FILE_HDR_SIZE];

  /* Read the first log file header to print a note if this is
  a recovery from a restored InnoDB Hot Backup */

  fil_io(OS_FILE_READ | OS_FILE_LOG, true, max_cp_group->space_id, 0, 0, LOG_FILE_HDR_SIZE, log_hdr_buf, max_cp_group);

  if (0 == memcmp(log_hdr_buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, (byte *)"ibbackup", (sizeof "ibbackup") - 1)) {
    /* This log file was created by ibbackup --restore: print
    a note to the user about it */

    ib_logger(
      ib_stream,
      "The log file was created by"
      " ibbackup --apply-log at\n"
      "%s\n",
      log_hdr_buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP
    );
    ib_logger(
      ib_stream,
      "NOTE: the following crash recovery"
      " is part of a normal restore.\n"
    );

    /* Wipe over the label now */

    memset(log_hdr_buf + LOG_FILE_WAS_CREATED_BY_HOT_BACKUP, ' ', 4);
    /* Write to the log file to wipe over the label */
    fil_io(OS_FILE_WRITE | OS_FILE_LOG, true, max_cp_group->space_id, 0, 0, IB_FILE_BLOCK_SIZE, log_hdr_buf, max_cp_group);
  }
}

/** Compare the checkpoint lsn with the lsn recorded in the data files
and start crash recovery if required. */
static void recv_init_crash_recovery(
  ib_recovery_t recovery,   /*!< in: recovery flag */
  uint64_t checkpoint_lsn,  /*!< in: checkpoint lsn */
  uint64_t min_flushed_lsn, /*!< in: min flushed lsn
                                                      from data files */
  uint64_t max_flushed_lsn
) /*!< in: max flushed lsn
                                                      from data files */
{
  /* NOTE: we always do a 'recovery' at startup, but only if
  there is something wrong we will print a message to the
  user about recovery: */

  if (checkpoint_lsn != max_flushed_lsn || checkpoint_lsn != min_flushed_lsn) {

    if (checkpoint_lsn < max_flushed_lsn) {
      ib_logger(
        ib_stream,
        "#########################"
        "#################################\n"
        "                         "
        "WARNING!\n"
        "The log sequence number"
        " in ibdata files is higher\n"
        "than the log sequence number"
        " in the ib_logfiles! Are you sure\n"
        "you are using the right"
        " ib_logfiles to start up"
        " the database?\n"
        "Log sequence number in"
        " ib_logfiles is %lu, log\n"
        "sequence numbers stamped"
        " to ibdata file headers are between\n"
        "%lu and %lu.\n"
        "#########################"
        "#################################\n",
        checkpoint_lsn,
        min_flushed_lsn,
        max_flushed_lsn
      );
    }

    if (!recv_needed_recovery) {
      ib_logger(
        ib_stream,
        "The log sequence number"
        " in ibdata files does not match\n"
        "the log sequence number"
        " in the ib_logfiles!\n"
      );

      recv_start_crash_recovery(recovery);
    }
  }

  if (!recv_needed_recovery) {
    /* Init the doublewrite buffer memory structure */
    trx_sys_doublewrite_init_or_restore_pages(false);
  }
}

db_err recv_recovery_from_checkpoint_start_func(ib_recovery_t recovery, lsn_t min_flushed_lsn, lsn_t max_flushed_lsn) {
  recv_sys_create();

  recv_sys_init(buf_pool_get_curr_size());

  if (recovery >= IB_RECOVERY_NO_LOG_REDO) {
    ib_logger(ib_stream, "The user has set IB_RECOVERY_NO_LOG_REDO on. Skipping log redo");

    return DB_SUCCESS;
  }

  recv_recovery_on = true;

  recv_sys->limit_lsn = IB_UINT64_T_MAX;

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

  recv_recover_from_ibbackup(max_cp_group);

  /* Start reading the log groups from the checkpoint lsn up. The
  variable contiguous_lsn contains an lsn up to which the log is
  known to be contiguously written to all log groups. */

  recv_sys->parse_start_lsn = checkpoint_lsn;
  recv_sys->scanned_lsn = checkpoint_lsn;
  recv_sys->scanned_checkpoint_no = 0;
  recv_sys->recovered_lsn = checkpoint_lsn;

  srv_start_lsn = checkpoint_lsn;

  auto up_to_date_group = max_cp_group;
  auto contiguous_lsn = ut_uint64_align_down(recv_sys->scanned_lsn, IB_FILE_BLOCK_SIZE);

  ut_ad(RECV_SCAN_SIZE <= log_sys->buf_size);

  lsn_t group_scanned_lsn{};

  for (auto group = UT_LIST_GET_FIRST(log_sys->log_groups); group != nullptr; group = UT_LIST_GET_NEXT(log_groups, group)) {

    auto old_scanned_lsn = recv_sys->scanned_lsn;

    recv_group_scan_log_recs(recovery, group, &contiguous_lsn, &group_scanned_lsn);

    group->scanned_lsn = group_scanned_lsn;

    if (old_scanned_lsn < group_scanned_lsn) {
      /* We found a more up-to-date group */

      up_to_date_group = group;
    }
  }

  recv_init_crash_recovery(recovery, checkpoint_lsn, min_flushed_lsn, max_flushed_lsn);

  /* We currently have only one log group */
  if (group_scanned_lsn < checkpoint_lsn) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      " ERROR: We were only able to scan the log up to"
      " %lu, but a checkpoint was at %lu. It is possible that"
      " the database is now corrupt!",
      group_scanned_lsn,
      checkpoint_lsn
    );
  }

  if (group_scanned_lsn < recv_max_page_lsn) {
    ut_print_timestamp(ib_stream);
    ib_logger(
      ib_stream,
      "ERROR: We were only able to scan the log up to %lu"
      " but a database page a had an lsn %lu. It is possible that the"
      " database is now corrupt!",
      group_scanned_lsn,
      recv_max_page_lsn
    );
  }

  if (recv_sys->recovered_lsn < checkpoint_lsn) {

    mutex_exit(&log_sys->mutex);

    ut_a(recv_sys->recovered_lsn >= LSN_MAX);

    return DB_SUCCESS;
  }

  /* Synchronize the uncorrupted log groups to the most up-to-date log
  group; we also copy checkpoint info to groups */

  log_sys->next_checkpoint_lsn = checkpoint_lsn;
  log_sys->next_checkpoint_no = checkpoint_no + 1;

  recv_synchronize_groups(up_to_date_group);

  if (!recv_needed_recovery) {
    ut_a(checkpoint_lsn == recv_sys->recovered_lsn);
  } else {
    srv_start_lsn = recv_sys->recovered_lsn;
  }

  log_sys->lsn = recv_sys->recovered_lsn;

  memcpy(log_sys->buf, recv_sys->last_block, IB_FILE_BLOCK_SIZE);

  log_sys->buf_free = ulint(log_sys->lsn % IB_FILE_BLOCK_SIZE);
  log_sys->buf_next_to_write = log_sys->buf_free;
  log_sys->written_to_some_lsn = log_sys->lsn;
  log_sys->written_to_all_lsn = log_sys->lsn;

  log_sys->last_checkpoint_lsn = checkpoint_lsn;

  log_sys->next_checkpoint_no = checkpoint_no + 1;

  mutex_enter(&recv_sys->mutex);

  recv_sys->apply_log_recs = true;

  mutex_exit(&recv_sys->mutex);

  mutex_exit(&log_sys->mutex);

  recv_lsn_checks_on = true;

  /* The database is now ready to start almost normal processing of user
  transactions: transaction rollbacks and the application of the log
  records in the hash table can be run in background. */

  return DB_SUCCESS;
}

void recv_recovery_from_checkpoint_finish(ib_recovery_t recovery) {
  /* Apply the hashed log records to the respective file pages */

  if (recovery < IB_RECOVERY_NO_LOG_REDO) {

    recv_apply_hashed_log_recs(false);
  }

#ifdef UNIV_DEBUG
  if (log_debug_writes) {
    ib_logger(ib_stream, "Log records applied to the database\n");
  }
#endif /* UNIV_DEBUG */

  if (recv_sys->found_corrupt_log) {

    ib_logger(
      ib_stream,
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

#ifndef UNIV_LOG_DEBUG
  recv_sys_debug_free();
#endif /* UNIV_LOG_DEBUG */

  /* Roll back any recovered data dictionary transactions, so
  that the data dictionary tables will be free of any locks.
  The data dictionary latch should guarantee that there is at
  most one data dictionary transaction active at a time. */
  trx_rollback_or_clean_recovered(false);
}

void recv_recovery_rollback_active() {
  int i;

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

    os_thread_create(trx_rollback_or_clean_all_recovered, (void *)&i, nullptr);
  }
}

void recv_reset_logs(uint64_t lsn, bool new_logs_created) {
  ut_ad(mutex_own(&log_sys->mutex));

  log_sys->lsn = ut_uint64_align_up(lsn, IB_FILE_BLOCK_SIZE);

  auto group = UT_LIST_GET_FIRST(log_sys->log_groups);

  while (group != nullptr) {
    group->lsn = log_sys->lsn;
    group->lsn_offset = LOG_FILE_HDR_SIZE;

    if (!new_logs_created) {
      recv_truncate_group(group, group->lsn, group->lsn, group->lsn, group->lsn);
    }

    group = UT_LIST_GET_NEXT(log_groups, group);
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
