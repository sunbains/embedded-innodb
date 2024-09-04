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

/** @file include/log0recv.h
Recovery

Created 9/20/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include "api0api.h"
#include "buf0dblwr.h"
#include "buf0types.h"
#include "log0log.h"
#include "srv0srv.h"
#include "ut0byte.h"

#include <unordered_map>

/**
 * Applies the hashed log records to the page, if the page lsn is less than
 * the lsn of a log record. This can be called when a buffer page has just been
 * read in, or also for a page already in the buffer pool.
 * 
 * @param[in] just_read_in      true if the i/o handler calls this for a
                                freshly read page
 * @param[in,out] block         buffer block
*/
void recv_recover_page(bool just_read_in, buf_block_t *block) noexcept;

/**
 * Recovers from a checkpoint. When this function returns, the database is able
 * to start processing of new user transactions, but the function
 * recv_recovery_from_checkpoint_finish should be called later to complete
 * the recovery and free the resources used in it.
 * 
 * @param[in,out] dblwr         Doublewrite buffer for recovering pages
 * @param[in] recovery          Recovery flag
 * @param[in] max_flushed_lsn   Max flushed lsn read from system.idb 
 * 
 * @return	error code or DB_SUCCESS
 */
db_err recv_recovery_from_checkpoint_start(DBLWR* dblwr, ib_recovery_t recovery, lsn_t max_flushed_lsn) noexcept;

/**
 * Completes recovery from a checkpoint.
 * 
 * @param[in,out] dblwr         Doublewrite buffer to use
 * @param[in] recovery          Recovery flag
 */
void recv_recovery_from_checkpoint_finish(DBLWR *dblwr, ib_recovery_t recovery) noexcept;

/**
 * Initiates the rollback of active transactions.
 */
void recv_recovery_rollback_active() noexcept;

/**
 * Resets the logs. The contents of log files will be lost!
 * 
 * @param[in] lsn               reset to this lsn rounded up to be divisible by
 *                              IB_FILE_BLOCK_SIZE, after which we add
 *		                          LOG_BLOCK_HDR_SIZE
 * @param[in] new_logs_created  true if resetting logs is done at the log
 *                              creation; false if it is done after archive
*		                            recovery
 */
void recv_reset_logs(lsn_t lsn, bool new_logs_created) noexcept;

/**
 * Reset the state of the recovery system variables.
 */
void recv_sys_var_init() noexcept;

/**
 * Empties the log record storage, applying them to appropriate pages.
 *
 * @param[in,out] dblwr         The doublewrite buffer to use
 * @param[in] flush_and_free_pages If true the application all file pages
 * are flushed to disk and invalidated in buffer pool: this alternative
 * means that no new log records can be generated during the application;
 * the caller must in this case own the log mutex;
*/
void recv_apply_log_recs(DBLWR *dblwr, bool flush_and_free_pages) noexcept;

/**
 * Block of log record data, variable size struct, see note.
 */
struct Log_record_data {

  /** @return string representation of the data.  */
  std::string to_string() const noexcept;

  /** Pointer to the next block or nullptr */
  Log_record_data *m_next;

  /* Note: the log record data is stored physically immediately after this
  struct, max amount RECV_DATA_BLOCK_SIZE bytes of it */
};

/** Parsed log log record */
struct Log_record {

  /** @return string representation of the data.  */
  std::string to_string() const noexcept;

  /** Log record type */
  mlog_type_t m_type{MLOG_UNKNOWN};

  /** Log record body length in bytes */
  uint32_t m_len{};

  /** start lsn of the log segment written by the mtr
  which generated this log record: NOTE that this is
  not necessarily the start lsn of this log record */
  lsn_t m_start_lsn{};

  /** end lsn of the log segment written by the mtr
  which generated this log record: NOTE that this is
  not necessarily the end lsn of this log record */
  lsn_t m_end_lsn{};

  /** Pointer to the next log record in the singly linked list */
  Log_record *m_next{};

  /** Chain of blocks containing the log record body */
  Log_record_data *m_data{};
};

/** States of recv_addr_struct */
enum Recv_addr_state {
  RECV_UNKNOWN = 0,

  /** Not yet processed */
  RECV_NOT_PROCESSED,

  /** Page is being read */
  RECV_BEING_READ,

  /** Log records are being applied on the page */
  RECV_BEING_PROCESSED,

  /** Log records have been applied on the page, or they have
  been discarded because the tablespace does not exist */
  RECV_PROCESSED
};

/** @return string presentation of Recv_addr_state */
std::string to_string(Recv_addr_state s) noexcept;

struct Page_log_records {
  /** @return string representation of the data.  */
  std::string to_string() const noexcept;

  /** Recovery state of the page */
  Recv_addr_state m_state{RECV_NOT_PROCESSED};

  /** Head of the singly linked list */
  Log_record *m_rec_head{};

  /** Tail of the singly linked list. */
  Log_record *m_rec_tail{};
};

/** Recovery system data structure */
struct Recv_sys {
  using Page_log_records_map = std::unordered_map<page_no_t, Page_log_records*>;
  using Tablespace_map = std::unordered_map<space_id_t, Page_log_records_map>;

  /** Constructor */
  Recv_sys() noexcept;

  /** Destructor.  */
  ~Recv_sys() noexcept;

  /**
  * Inits the recovery system for a recovery operation
  * 
  * @param[in] size              Available memory in bytes
  */
  void open(ulint size) noexcept;

  /**
   * Free the recv_sys recovery related data.
   */
  void close() noexcept;

  /**
   * Empties the hash table when it has been fully processed.
   */
  void reset() noexcept;

  /**
   * Clears the log records from the map.
   */
  void clear_log_records() noexcept;

  /**
  * Adds a new log record to the map of log records.
  * 
  * @param type The type of the log record.
  * @param space The space id.
  * @param page_no The page number.
  * @param body Pointer to the log record body.
  * @param rec_end Pointer to the end of the log record.
  * @param start_lsn The start LSN of the mtr.
  * @param end_lsn The end LSN of the mtr.
  */
  void add_log_record(mlog_type_t type, space_id_t space, page_no_t page_no, byte *body, byte *rec_end, lsn_t start_lsn, lsn_t end_lsn) noexcept;

  /** @return string representation of the data.  */
  std::string to_string() const noexcept;

  /** mutex protecting the fields apply_log_recs, n_addrs, and
  the state field in each recv_addr struct */
  mutable mutex_t m_mutex{};

  /** this is true when log rec application to pages is allowed;
  this flag tells the i/o-handler if it should do log record application */
  bool m_apply_log_recs{};

  /** this is true when a log rec application batch is running */
  bool m_apply_batch_on{};

  /** log sequence number */
  lsn_t m_lsn{};

  /** size of the log buffer when the database last time wrote to the log */
  ulint m_last_log_buf_size{};

  /** possible incomplete last recovered log block */
  byte *m_last_block{};

  /** the nonaligned start address of the preceding buffer */
  byte *m_last_block_buf_start{};

  /** buffer for parsing log records */
  byte *m_buf{};

  /** amount of data in buf */
  ulint m_len{};

  /** this is the lsn from which we were able to start parsing log
  records and adding them to the hash table; zero if a suitable
  start point not found yet */
  lsn_t m_parse_start_lsn{};

  /** the log data has been scanned up to this lsn */
  lsn_t m_scanned_lsn{};

  /** the log data has been scanned up to this checkpoint number (lowest 4 bytes) */
  ulint m_scanned_checkpoint_no{};

  /** start offset of non-parsed log records in buf */
  ulint m_recovered_offset{};

  /** the log records have been parsed up to this lsn */
  lsn_t m_recovered_lsn{};

  /** recovery should be made at most up to this lsn */
  lsn_t m_limit_lsn{};

  /** this is set to true if we during log scan find a corrupt log block, or a corrupt
  log record, or there is a log parsing buffer overflow */
  bool m_found_corrupt_log{};

  /** memory heap of log records and file addresses*/
  mem_heap_t *m_heap{};

  /** Parsed log records that need to be applied to the tablepsace pages. */
  Tablespace_map m_log_records{};

  /** Number of log records parsed and stored in m_log_records so far. */
  ulint m_n_log_records{};
};

/** The recovery system */
extern Recv_sys *recv_sys;

/** true when applying redo log records during crash recovery; false
otherwise.  Note that this is false while a background thread is
rolling back incomplete transactions. */
extern bool recv_recovery_on;

/** true when recv_init_crash_recovery() has been called. */
extern bool recv_needed_recovery;

/** true if buf_page_is_corrupted() should check if the log sequence
number (FIL_PAGE_LSN) is in the future.  Initially false, and set by
recv_recovery_from_checkpoint_start_func(). */
extern bool recv_lsn_checks_on;

extern ib_cb_t recv_pre_rollback_hook;

/** Maximum page number encountered in the redo log */
extern ulint recv_max_parsed_page_no;

/** This many frames must be left free in the buffer pool when we scan
the log and store the scanned log records in the buffer pool: we will
use these free frames to read in pages when we start applying the
log records to the database. */
extern ulint recv_n_pool_free_frames;

/** Size of the parsing buffer; it must accommodate RECV_SCAN_SIZE many times! */
constexpr ulint RECV_PARSING_BUF_SIZE = 2 * 1024 * 1024;

/** Size of block reads when the log groups are scanned forward to do a roll-forward */
constexpr ulint RECV_SCAN_SIZE = 4 * UNIV_PAGE_SIZE;
