/****************************************************************************
Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.
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

/** @file trx/trx0sys.c
Transaction system

Created 3/26/1996 Heikki Tuuri
*******************************************************/

#include "trx0sys.h"

#include "fsp0fsp.h"
#include "log0log.h"
#include "mtr0log.h"
#include "os0file.h"
#include "read0read.h"
#include "srv0srv.h"
#include "trx0purge.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0undo.h"

/** The file format tag structure with id and name. All changes to this
data structure are covered by the dictionary mutex. */
struct file_format_t {
  /** id of the file format */
  ulint id;

  /** Text representation of the file format */
  const char *name;
};

/** The transaction system */
trx_sys_t *trx_sys = nullptr;

/** The doublewrite buffer */
trx_doublewrite_t *trx_doublewrite = nullptr;

/** The following is set to true when we are upgrading from pre-4.1
format data files to the multiple tablespaces format data files */
bool trx_doublewrite_must_reset_space_ids = false;

/** Set to true when the doublewrite buffer is being created */
bool trx_doublewrite_buf_is_being_created = false;

/** The following is true when we are using the database in the
post-4.1 format, i.e., we have successfully upgraded, or have created
a new database installation */
bool trx_sys_multiple_tablespace_format = false;

/** List of animal names representing file format. */
static const char *file_format_name_map[] = { "default" };

/** The number of elements in the file format name array. */
static const ulint FILE_FORMAT_NAME_N = sizeof(file_format_name_map) / sizeof(file_format_name_map[0]);

/** This is used to track the maximum file format id known to InnoDB. It's
updated via SET GLOBAL file_format_check = 'x' or when we open
or create a table. */
static file_format_t file_format_max;

void trx_sys_var_init() {
  trx_sys = nullptr;
  trx_doublewrite = nullptr;

  trx_doublewrite_must_reset_space_ids = false;
  trx_sys_multiple_tablespace_format = false;

  memset(&file_format_max, 0x0, sizeof(file_format_max));
}

bool trx_doublewrite_page_inside(page_no_t page_no) {
  if (trx_doublewrite == nullptr) {

    return false;
  }

  if (page_no >= trx_doublewrite->block1 && page_no < trx_doublewrite->block1 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {

    return true;
  }

  if (page_no >= trx_doublewrite->block2 && page_no < trx_doublewrite->block2 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {

    return true;
  } else {

    return false;
  }
}

/**
 * Creates or initialializes the doublewrite buffer at a database start.
 *
 * @param doublewrite Pointer to the doublewrite buffer header on the transaction system page.
 */
static void trx_doublewrite_init(byte *doublewrite) {
  trx_doublewrite = static_cast<trx_doublewrite_t *>(mem_alloc(sizeof(trx_doublewrite_t)));

  /* Since we now start to use the doublewrite buffer, no need to call
  fsync() after every write to a data file */

  mutex_create(&trx_doublewrite->mutex, IF_DEBUG("dblwr_mutex",) IF_SYNC_DEBUG(SYNC_DOUBLEWRITE,) Source_location{});

  trx_doublewrite->first_free = 0;

  trx_doublewrite->block1 = mach_read_from_4(doublewrite + TRX_SYS_DOUBLEWRITE_BLOCK1);

  trx_doublewrite->block2 = mach_read_from_4(doublewrite + TRX_SYS_DOUBLEWRITE_BLOCK2);

  trx_doublewrite->write_buf_unaligned = static_cast<byte *>(ut_new((1 + 2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) * UNIV_PAGE_SIZE));

  trx_doublewrite->write_buf = static_cast<byte *>(ut_align(trx_doublewrite->write_buf_unaligned, UNIV_PAGE_SIZE));

  trx_doublewrite->buf_block_arr = static_cast<buf_page_t **>(mem_alloc(2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * sizeof(void *)));
}

void trx_sys_mark_upgraded_to_multiple_tablespaces() {
  mtr_t mtr;

  /* We upgraded to 4.1.x and reset the space id fields in the
  doublewrite buffer. Let us mark to the trx_sys header that the upgrade
  has been done. */

  mtr_start(&mtr);

  Buf_pool::Request req {
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = { TRX_SYS_SPACE, TRX_SYS_PAGE_NO },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = &mtr
  };

  auto block = srv_buf_pool->get(req, nullptr);
  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_NO_ORDER_CHECK));

  auto doublewrite = block->get_frame() + TRX_SYS_DOUBLEWRITE;

  mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED, TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N, MLOG_4BYTES, &mtr);
  mtr_commit(&mtr);

  /* Flush the modified pages to disk and make a checkpoint */
  log_make_checkpoint_at(IB_UINT64_T_MAX, true);

  trx_sys_multiple_tablespace_format = true;
}

db_err trx_sys_create_doublewrite_buf() {
  ut_a(trx_doublewrite == nullptr);

  mtr_t mtr;

  Buf_pool::Request req {
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = { TRX_SYS_SPACE, TRX_SYS_PAGE_NO },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = &mtr
  };

  mtr_start(&mtr);

  trx_doublewrite_buf_is_being_created = true;

  auto block = srv_buf_pool->get(req, nullptr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_NO_ORDER_CHECK));

  auto doublewrite = block->get_frame() + TRX_SYS_DOUBLEWRITE;

  if (mach_read_from_4(doublewrite + TRX_SYS_DOUBLEWRITE_MAGIC) == TRX_SYS_DOUBLEWRITE_MAGIC_N) {
    mtr_commit(&mtr);

    trx_doublewrite_init(doublewrite);
    trx_doublewrite_buf_is_being_created = false;

    return DB_SUCCESS;
  }

  log_info("Doublewrite buffer not found: creating new");

  if (srv_buf_pool->get_curr_size() < ((2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE + FSP_EXTENT_SIZE / 2 + 100) * UNIV_PAGE_SIZE)) {
    log_err(
      "Cannot create doublewrite buffer: you must increase your buffer pool size."
      " Cannot continue operation."
    );

    return DB_FATAL;
  }

  auto block2 = fseg_create(TRX_SYS_SPACE, TRX_SYS_PAGE_NO, TRX_SYS_DOUBLEWRITE + TRX_SYS_DOUBLEWRITE_FSEG, &mtr);

  /* fseg_create acquires a second latch on the page, therefore we must declare it: */

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block2, SYNC_NO_ORDER_CHECK));

  if (block2 == nullptr) {
    log_err(
      "Cannot create doublewrite buffer:  you must increase your tablespace size."
      " Cannot continue operation."
    );

    /* We need to exit without committing the mtr to prevent its modifications to
    the database getting to disk */

    return DB_FATAL;
  }

  page_no_t prev_page_no{};
  auto fseg_header = block->get_frame() + TRX_SYS_DOUBLEWRITE + TRX_SYS_DOUBLEWRITE_FSEG;

  for (ulint i = 0; i < 2 * TRX_SYS_DOUBLEWRITE_BLOCK_SIZE + FSP_EXTENT_SIZE / 2; i++) {
    auto page_no = fseg_alloc_free_page(fseg_header, prev_page_no + 1, FSP_UP, &mtr);

    if (page_no == FIL_NULL) {
      log_err(
        "Cannot create doublewrite buffer: you must increase your tablespace size."
        " Cannot continue operation."
      );

      return DB_FATAL;
    }

    /* We read the allocated pages to the buffer pool; when they are written to disk in a flush,
    the space id and page number fields are also written to the pages. When we at database startup
    read pages from the doublewrite buffer, we know that if the space id and page number in them
    are the same as the page position in the tablespace, then the page has not been written to in
    doublewrite. */

    IF_SYNC_DEBUG({
      Buf_pool::Request req {
        .m_rw_lock = RW_X_LATCH,
        .m_page_id = { TRX_SYS_SPACE, page_no },
        .m_mode = BUF_GET,
        .m_file = __FILE__,
        .m_line = __LINE__,
        .m_mtr = &mtr
      };
      auto new_block = buf_pool->get(req, nullptr);
      buf_block_dbg_add_level(IF_SYNC_DEBUG(new_block, SYNC_NO_ORDER_CHECK));
    })

    if (i == FSP_EXTENT_SIZE / 2) {
      ut_a(page_no == FSP_EXTENT_SIZE);
      mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_BLOCK1, page_no, MLOG_4BYTES, &mtr);
      mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_REPEAT + TRX_SYS_DOUBLEWRITE_BLOCK1, page_no, MLOG_4BYTES, &mtr);
    } else if (i == FSP_EXTENT_SIZE / 2 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
      ut_a(page_no == 2 * FSP_EXTENT_SIZE);
      mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_BLOCK2, page_no, MLOG_4BYTES, &mtr);
      mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_REPEAT + TRX_SYS_DOUBLEWRITE_BLOCK2, page_no, MLOG_4BYTES, &mtr);
    } else if (i > FSP_EXTENT_SIZE / 2) {
      ut_a(page_no == prev_page_no + 1);
    }

    prev_page_no = page_no;
  }

  mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_MAGIC, TRX_SYS_DOUBLEWRITE_MAGIC_N, MLOG_4BYTES, &mtr);

  mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_MAGIC + TRX_SYS_DOUBLEWRITE_REPEAT, TRX_SYS_DOUBLEWRITE_MAGIC_N, MLOG_4BYTES, &mtr);

  mlog_write_ulint(doublewrite + TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED, TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N, MLOG_4BYTES, &mtr);

  mtr_commit(&mtr);

  /* Flush the modified pages to disk and make a checkpoint */
  log_make_checkpoint_at(IB_UINT64_T_MAX, true);

  log_info("Doublewrite buffer created");

  trx_sys_multiple_tablespace_format = true;

  trx_doublewrite_init(doublewrite);

  trx_doublewrite_buf_is_being_created = false;

  return DB_SUCCESS;
}

void trx_sys_doublewrite_init_or_restore_pages(bool restore_corrupt_pages) {
  byte *buf;
  ulint block1;
  ulint block2;
  page_no_t source_page_no;
  space_id_t space_id;
  page_no_t page_no;

  /* We do the file i/o past the buffer pool */

  auto unaligned_read_buf = static_cast<byte *>(ut_new(2 * UNIV_PAGE_SIZE));
  auto read_buf = static_cast<byte *>(ut_align(unaligned_read_buf, UNIV_PAGE_SIZE));

  /* Read the trx sys header to check if we are using the doublewrite buffer */

  srv_fil->io(IO_request::Sync_read, false, TRX_SYS_SPACE, TRX_SYS_PAGE_NO, 0, UNIV_PAGE_SIZE, read_buf, nullptr);

  page_t *page;
  auto doublewrite = read_buf + TRX_SYS_DOUBLEWRITE;

  if (mach_read_from_4(doublewrite + TRX_SYS_DOUBLEWRITE_MAGIC) == TRX_SYS_DOUBLEWRITE_MAGIC_N) {
    /* The doublewrite buffer has been created */

    trx_doublewrite_init(doublewrite);

    block1 = trx_doublewrite->block1;
    block2 = trx_doublewrite->block2;

    buf = trx_doublewrite->write_buf;
  } else {
    goto leave_func;
  }

  ut_a(mach_read_from_4(doublewrite + TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED) == TRX_SYS_DOUBLEWRITE_SPACE_ID_STORED_N);
  trx_sys_multiple_tablespace_format = true;

  /* Read the pages from the doublewrite buffer to memory */

  srv_fil->io(
    IO_request::Sync_read,
    false,
    TRX_SYS_SPACE,
    block1,
    0,
    TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE,
    buf,
    nullptr);

  srv_fil->io(
    IO_request::Sync_read,
    false,
    TRX_SYS_SPACE,
    block2,
    0,
    TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE,
    buf + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * UNIV_PAGE_SIZE,
    nullptr
  );

  /* Check if any of these pages is half-written in data files, in the intended
   * position */

  page = buf;

  for (ulint i = 0; i < TRX_SYS_DOUBLEWRITE_BLOCK_SIZE * 2; i++) {

    page_no = mach_read_from_4(page + FIL_PAGE_OFFSET);

    if (trx_doublewrite_must_reset_space_ids) {

      space_id = 0;
      mach_write_to_4(page + FIL_PAGE_SPACE_ID, 0);
      /* We do not need to calculate new checksums for the
      pages because the field .._SPACE_ID does not affect
      them. Write the page back to where we read it from. */

      if (i < TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) {
        source_page_no = block1 + i;
      } else {
        source_page_no = block2 + i - TRX_SYS_DOUBLEWRITE_BLOCK_SIZE;
      }

      srv_fil->io(IO_request::Sync_write, false, 0, 0, source_page_no, UNIV_PAGE_SIZE, page, nullptr);
      /* printf("Resetting space id in page %lu\n",
      source_page_no); */
    } else {
      space_id = mach_read_from_4(page + FIL_PAGE_SPACE_ID);
    }

    if (!restore_corrupt_pages) {
      /* The database was shut down gracefully: no need to
      restore pages */

    } else if (!srv_fil->tablespace_exists_in_mem(space_id)) {
      /* Maybe we have dropped the single-table tablespace
      and this page once belonged to it: do nothing */

    } else if (!srv_fil->check_adress_in_tablespace(space_id, page_no)) {
      log_warn(std::format(
        "A page in the doublewrite buffer is not within space bounds; space id {}"
        " page number {}, page {} in doublewrite buf.",
        space_id,
        page_no,
        i
      ));

    } else if (space_id == TRX_SYS_SPACE &&
               ((page_no >= block1 && page_no < block1 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE) ||
                (page_no >= block2 && page_no < block2 + TRX_SYS_DOUBLEWRITE_BLOCK_SIZE))) {

      /* It is an unwritten doublewrite buffer page: do nothing */
    } else {
      /* Read in the actual page from the file */
      srv_fil->io(IO_request::Sync_read, false, space_id, page_no, 0, UNIV_PAGE_SIZE, read_buf, nullptr);

      /* Check if the page is corrupt */

      if (unlikely(srv_buf_pool->is_corrupted(read_buf))) {

        log_warn(std::format(
          "Database page corruption or a failed file read of space {} page {}."
          " Trying to recover it from the doublewrite buffer.",
          space_id,
          page_no
        ));

        if (srv_buf_pool->is_corrupted(page)) {
          ib_logger(ib_stream, "Dump of the page:\n");
          buf_page_print(read_buf, 0);
          log_warn("Dump of corresponding page in doublewrite buffer:");
          buf_page_print(page, 0);

          log_fatal(
            "Also the page in the doublewrite buffer is corrupt."
            " Cannot continue operation. You can try to recover"
            " the database with the option: force_recovery=6"
          );
        }

        /* Write the good page from the doublewrite buffer to the intended
         * position */

        srv_fil->io(IO_request::Sync_write, false, space_id, page_no, 0, UNIV_PAGE_SIZE, page, nullptr);

        log_info("Recovered the page from the doublewrite buffer.");
      }
    }

    page += UNIV_PAGE_SIZE;
  }

  srv_fil->flush_file_spaces(FIL_TABLESPACE);

leave_func:
  ut_delete(unaligned_read_buf);
}

bool trx_in_trx_list(trx_t *in_trx) {
  ut_ad(mutex_own(&(kernel_mutex)));

  for (auto trx : trx_sys->trx_list) {
    if (trx == in_trx) {

      return true;
    }
  }

  return false;
}

void trx_sys_flush_max_trx_id() {
  ut_ad(mutex_own(&kernel_mutex));

  mtr_t mtr;

  mtr_start(&mtr);

  auto sys_header = trx_sysf_get(&mtr);

  mlog_write_uint64(sys_header + TRX_SYS_TRX_ID_STORE, trx_sys->max_trx_id, &mtr);
  mtr_commit(&mtr);
}

ulint trx_sysf_rseg_find_free(mtr_t *mtr) {
  ut_ad(mutex_own(&kernel_mutex));

  auto sys_header = trx_sysf_get(mtr);

  for (ulint i = 0; i < TRX_SYS_N_RSEGS; i++) {

    auto page_no = trx_sysf_rseg_get_page_no(sys_header, i, mtr);

    if (page_no == FIL_NULL) {

      return i;
    }
  }

  return ULINT_UNDEFINED;
}

/**
 * Creates the file page for the transaction system. This function is called
 * only at the database creation, before trx_sys_init.
 * 
 * @param[in,out] mtr              Mini-transaction covering the operation.
 */
static void trx_sysf_create(mtr_t *mtr) {
  ut_ad(mtr != nullptr);

  /* Note that below we first reserve the file space x-latch, and
  then enter the kernel: we must do it in this order to conform
  to the latching order rules. */

  mtr_x_lock(srv_fil->space_get_latch(TRX_SYS_SPACE), mtr);
  mutex_enter(&kernel_mutex);

  /* Create the trx sys file block in a new allocated file segment */
  auto block = fseg_create(TRX_SYS_SPACE, 0, TRX_SYS + TRX_SYS_FSEG_HEADER, mtr);

  buf_block_dbg_add_level(IF_SYNC_DEBUG(block, SYNC_TRX_SYS_HEADER));

  ut_a(block->get_page_no() == TRX_SYS_PAGE_NO);

  auto page = block->get_frame();

  mlog_write_ulint(page + FIL_PAGE_TYPE, FIL_PAGE_TYPE_TRX_SYS, MLOG_2BYTES, mtr);

  /* Reset the doublewrite buffer magic number to zero so that we
  know that the doublewrite buffer has not yet been created (this
  suppresses a Valgrind warning) */

  mlog_write_ulint(page + TRX_SYS_DOUBLEWRITE + TRX_SYS_DOUBLEWRITE_MAGIC, 0, MLOG_4BYTES, mtr);

  auto sys_header = trx_sysf_get(mtr);

  /* Start counting transaction ids from number 1 up */
  mlog_write_uint64(sys_header + TRX_SYS_TRX_ID_STORE, 1, mtr);

  /* Reset the rollback segment slots */
  for (ulint i = 0; i < TRX_SYS_N_RSEGS; i++) {

    trx_sysf_rseg_set_space(sys_header, i, ULINT_UNDEFINED, mtr);
    trx_sysf_rseg_set_page_no(sys_header, i, FIL_NULL, mtr);
  }

  /* The remaining area (up to the page trailer) is uninitialized.
  Silence Valgrind warnings about it. */
  UNIV_MEM_VALID(
    sys_header + (TRX_SYS_RSEGS + TRX_SYS_N_RSEGS * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE),
    (UNIV_PAGE_SIZE - FIL_PAGE_DATA_END - (TRX_SYS_RSEGS + TRX_SYS_N_RSEGS * TRX_SYS_RSEG_SLOT_SIZE + TRX_SYS_RSEG_SPACE)) + page -
      sys_header
  );

  /* Create the first rollback segment in the SYSTEM tablespace */
  ulint slot_no;
  auto page_no = trx_rseg_header_create(TRX_SYS_SPACE, ULINT_MAX, &slot_no, mtr);

  ut_a(slot_no == TRX_SYS_SYSTEM_RSEG_ID);
  ut_a(page_no != FIL_NULL);

  mutex_exit(&kernel_mutex);
}

void trx_sys_init_at_db_start(ib_recovery_t recovery) {
  const char *unit = "";
  mtr_t mtr;

  mtr_start(&mtr);

  ut_ad(trx_sys == nullptr);

  mutex_enter(&kernel_mutex);

  trx_sys = static_cast<trx_sys_t *>(mem_alloc(sizeof(trx_sys_t)));

  UT_LIST_INIT(trx_sys->client_trx_list);

  auto sys_header = trx_sysf_get(&mtr);

  trx_rseg_list_and_array_init(recovery, sys_header, &mtr);

  trx_sys->latest_rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);

  /* VERY important: after the database is started, max_trx_id value is
  divisible by TRX_SYS_TRX_ID_WRITE_MARGIN, and the 'if' in
  trx_sys_get_new_trx_id will evaluate to true when the function
  is first time called, and the value for trx id will be written
  to the disk-based header! Thus trx id values will not overlap when
  the database is repeatedly started! */

  trx_sys->max_trx_id = ut_uint64_align_up(mtr_read_uint64(sys_header + TRX_SYS_TRX_ID_STORE, &mtr), TRX_SYS_TRX_ID_WRITE_MARGIN) +
                        2 * TRX_SYS_TRX_ID_WRITE_MARGIN;


  trx_dummy_sess = sess_open();
  trx_lists_init_at_db_start(recovery);

  int64_t rows_to_undo = 0;

  if (UT_LIST_GET_LEN(trx_sys->trx_list) > 0) {
    auto trx = UT_LIST_GET_FIRST(trx_sys->trx_list);

    for (;;) {

      if (trx->m_conc_state != TRX_PREPARED) {
        rows_to_undo += trx->undo_no;
      }

      trx = UT_LIST_GET_NEXT(trx_list, trx);

      if (!trx) {
        break;
      }
    }

    if (rows_to_undo > 1000000000) {
      unit = "M";
      rows_to_undo = rows_to_undo / 1000000;
    }

    log_info(std::format(
      "{} transaction(s) which must be rolled back or cleaned up"
      " in total {}s row operations to undo",
      UT_LIST_GET_LEN(trx_sys->trx_list),
      rows_to_undo,
      unit
    ));

    log_info(std::format("Trx id counter is {}", TRX_ID_PREP_PRINTF(trx_sys->max_trx_id)));
  }

  UT_LIST_INIT(trx_sys->view_list);

  trx_purge_sys_create();

  mutex_exit(&kernel_mutex);

  mtr_commit(&mtr);
}

void trx_sys_create(ib_recovery_t recovery) {
  mtr_t mtr;

  mtr_start(&mtr);

  trx_sysf_create(&mtr);

  mtr_commit(&mtr);

  trx_sys_init_at_db_start(recovery);
}

/**
 * Sets the maximum file format name for a given file format ID.
 *
 * @param[in] format_id The file format ID.
 * @param[out] name A pointer to the maximum file format name. Can be nullptr.
 *
 * @return True if the operation was successful, false otherwise.
 */
static bool trx_sys_file_format_max_write(ulint format_id, const char **name) {
  mtr_t mtr;

  mtr_start(&mtr);

  Buf_pool::Request req {
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = { TRX_SYS_SPACE, TRX_SYS_PAGE_NO },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = &mtr
  };

  auto block = srv_buf_pool->get(req, nullptr);

  file_format_max.id = format_id;
  file_format_max.name = trx_sys_file_format_id_to_name(format_id);

  auto ptr = block->get_frame() + TRX_SYS_FILE_FORMAT_TAG;
  auto tag_value_low = format_id + TRX_SYS_FILE_FORMAT_TAG_MAGIC_N_LOW;

  if (name != nullptr) {
    *name = file_format_max.name;
  }

  mlog_write_uint64(ptr, (uint64_t(TRX_SYS_FILE_FORMAT_TAG_MAGIC_N_HIGH) << 32) | tag_value_low, &mtr);

  mtr_commit(&mtr);

  return true;
}

/** Read the file format tag.
@return	the file format */
static ulint trx_sys_file_format_max_read() {
  mtr_t mtr;

  /* Since this is called during the startup phase it's safe to
  read the value without a covering mutex. */
  mtr_start(&mtr);

  Buf_pool::Request req {
    .m_rw_latch = RW_X_LATCH,
    .m_page_id = { TRX_SYS_SPACE, TRX_SYS_PAGE_NO },
    .m_mode = BUF_GET,
    .m_file = __FILE__,
    .m_line = __LINE__,
    .m_mtr = &mtr
  };

  auto block = srv_buf_pool->get(req, nullptr);

  const auto ptr = block->get_frame() + TRX_SYS_FILE_FORMAT_TAG;

  auto file_format_id = mach_read_from_8(ptr);

  mtr_commit(&mtr);

  auto format_id = file_format_id - TRX_SYS_FILE_FORMAT_TAG_MAGIC_N_LOW;

  if (file_format_id != TRX_SYS_FILE_FORMAT_TAG_MAGIC_N_HIGH || format_id >= FILE_FORMAT_NAME_N) {

    /* Either it has never been tagged, or garbage in it.  Reset the tag in either case. */
    format_id = DICT_TF_FORMAT_51;
    trx_sys_file_format_max_write(format_id, nullptr);
  }

  return format_id;
}

const char *trx_sys_file_format_id_to_name(const ulint id) {
  if (!(id < FILE_FORMAT_NAME_N)) {
    /* unknown id */
    return ("Unknown");
  }

  return file_format_name_map[id];
}

ulint trx_sys_file_format_name_to_id(const char *format_name) {
  char *endp;

  ut_a(format_name != nullptr);

  /* The format name can contain the format id itself instead of
  the name and we check for that. */
  const auto format_id = (ulint)strtoul(format_name, &endp, 10);

  /* Check for valid parse. */
  if (*endp == '\0' && *format_name != '\0') {

    if (format_id <= DICT_TF_FORMAT_MAX) {

      return format_id;
    }
  } else {

    for (ulint i{}; i <= DICT_TF_FORMAT_MAX; ++i) {
      auto name = trx_sys_file_format_id_to_name(i);

      if (!strcasecmp(format_name, name)) {
        return i;
      }
    }
  }

  return DICT_TF_FORMAT_MAX + 1;
}

db_err trx_sys_file_format_max_check(ulint max_format_id) {
  /* Check the file format in the tablespace. Do not try to
  recover if the file format is not supported by the engine
  unless forced by the user. */
  auto format_id = trx_sys_file_format_max_read();

  log_info("Highest supported file format is ", trx_sys_file_format_id_to_name(DICT_TF_FORMAT_MAX));

  ut_a(format_id <= DICT_TF_FORMAT_MAX);

  /* We don't need a mutex here, as this function should only be called once at start up. */
  file_format_max.id = format_id;
  file_format_max.name = trx_sys_file_format_id_to_name(format_id);

  return DB_SUCCESS;
}

bool trx_sys_file_format_max_set(ulint format_id, const char **name) {
  ut_a(format_id <= DICT_TF_FORMAT_MAX);

  /* Only update if not already same value. */
  return format_id != file_format_max.id ? trx_sys_file_format_max_write(format_id, name) : false;
}

void trx_sys_file_format_tag_init() {
  auto format_id = trx_sys_file_format_max_read();

  /* If format_id is not set then set it to the minimum. */
  if (format_id == ULINT_UNDEFINED) {
    trx_sys_file_format_max_set(DICT_TF_FORMAT_51, nullptr);
  }
}

void trx_sys_file_format_init() {
  /* We don't need a mutex here, as this function should only
  be called once at start up. */
  file_format_max.id = DICT_TF_FORMAT_51;

  file_format_max.name = trx_sys_file_format_id_to_name(file_format_max.id);
}

bool trx_sys_file_format_max_upgrade(const char **name, ulint format_id) {
  ut_a(file_format_max.name != nullptr);
  ut_a(format_id <= DICT_TF_FORMAT_MAX);

  return format_id > file_format_max.id ? trx_sys_file_format_max_write(format_id, name) : false;
}

void trx_sys_close() {
  ut_ad(trx_sys != nullptr);

  /* Check that all read views are closed except read view owned
  by a purge. */

  if (UT_LIST_GET_LEN(trx_sys->view_list) > 1) {
    log_err(std::format(
      "All read views were not closed before shutdown: {} read views open",
      UT_LIST_GET_LEN(trx_sys->view_list) - 1
    ));
  }

  sess_close(trx_dummy_sess);
  trx_dummy_sess = nullptr;

  trx_purge_sys_close();

  /* This is required only because it's a pre-condition for many
  of the functions that we need to call. */
  mutex_enter(&kernel_mutex);

  /* Free the double write data structures. */
  ut_delete(trx_doublewrite->write_buf_unaligned);
  trx_doublewrite->write_buf_unaligned = nullptr;

  mem_free(trx_doublewrite->buf_block_arr);
  trx_doublewrite->buf_block_arr = nullptr;

  mutex_free(&trx_doublewrite->mutex);
  mem_free(trx_doublewrite);
  trx_doublewrite = nullptr;

  /* End freeing of doublewrite buffer data structures. */

  /* There can't be any active transactions. */
  auto rseg = UT_LIST_GET_FIRST(trx_sys->rseg_list);

  while (rseg != nullptr) {
    auto prev_rseg = rseg;

    rseg = UT_LIST_GET_NEXT(rseg_list, prev_rseg);

    UT_LIST_REMOVE(trx_sys->rseg_list, prev_rseg);

    trx_rseg_mem_free(prev_rseg);
  }

  auto view = UT_LIST_GET_FIRST(trx_sys->view_list);

  while (view != nullptr) {
    auto prev_view = view;

    view = UT_LIST_GET_NEXT(view_list, prev_view);

    /* Views are allocated from the trx_sys->global_read_view_heap.
    So, we simply remove the element here. */
    UT_LIST_REMOVE(trx_sys->view_list, prev_view);
}

  ut_a(UT_LIST_GET_LEN(trx_sys->trx_list) == 0);
  ut_a(UT_LIST_GET_LEN(trx_sys->rseg_list) == 0);
  ut_a(UT_LIST_GET_LEN(trx_sys->view_list) == 0);
  ut_a(UT_LIST_GET_LEN(trx_sys->client_trx_list) == 0);

  mem_free(trx_sys);

  trx_sys = nullptr;
  mutex_exit(&kernel_mutex);
}

void trx_sys_file_format_close() { }
