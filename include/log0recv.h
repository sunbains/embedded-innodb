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

/** @file include/log0recv.h
Recovery

Created 9/20/1997 Heikki Tuuri
*******************************************************/

#pragma once

#include "innodb0types.h"

#include <condition_variable>
#include <list>
#include <memory>
#include <vector>
#include "api0api.h"
#include "buf0types.h"
#include "hash0hash.h"
#include "log0log.h"
#include "srv0srv.h"
#include "ut0byte.h"

using namespace std::chrono_literals;
struct recv_t;
class append_only_queue;

/** Recovers from a checkpoint. When this function returns, the database is able
to start processing of new user transactions, but the function
recv_recovery_from_checkpoint_finish should be called later to complete
the recovery and free the resources used in it.
@param[in] recovery             Recovery flag
@param[in] min_flushed_lsn,     Flushed lsn from data files
@param[in] max_flushed_lsn      Max flushed lsn from data files
@return	error code or DB_SUCCESS */
db_err recv_recovery_from_checkpoint_start_func(ib_recovery_t recovery, lsn_t min_flushed_lsn, lsn_t max_flushed_lsn);

/** Wrapper for recv_recovery_from_checkpoint_start_func().
Recovers from a checkpoint. When this function returns, the database is able
to start processing of new user transactions, but the function
recv_recovery_from_checkpoint_finish should be called later to complete
the recovery and free the resources used in it.
@param lim	ignored: recover up to this log sequence number if possible
@param min	in: minimum flushed log sequence number from data files
@param max	in: maximum flushed log sequence number from data files
@return	error code or DB_SUCCESS */
#define recv_recovery_from_checkpoint_start(recv, type, lim, min, max) recv_recovery_from_checkpoint_start_func(recv, min, max)

/** Completes recovery from a checkpoint.
@param[in] recovery             Recovery flag */
void recv_recovery_from_checkpoint_finish(ib_recovery_t recovery);

/** Initiates the rollback of active transactions. */
void recv_recovery_rollback_active();

/** Scans log from a buffer and stores new log data to the parsing buffer.
Parses and hashes the log records if new data found.  This function will
apply log records automatically when the hash table becomes full.
@return true if limit_lsn has been reached, or not able to scan any
more in this log group.
@param[in] recovery             Recovery flag
@param[in] store_to_hash        true if the records should be stored to the
                                hashtable; this is set to false if just debug
				checking is needed
@param[in] buf                  Buffer containing a log segment or garbage
@param[in] len                  Buffer length
@param[in] start_lsn            Buffer start lsn
@param[in,out] contiguous_lsn   It is known that all log groups contain
                                contiguouslog data up to this lsn
@param[out] group_scanned_lsn   Scanning succeeded up to this lsn */
bool recv_scan_log_recs(
  ib_recovery_t recovery, bool store_to_hash, const byte *buf, ulint len, lsn_t start_lsn,
  lsn_t *contiguous_lsn, lsn_t *group_scanned_lsn, append_only_queue records
);

/** Resets the logs. The contents of log files will be lost!
@param[in] lsn                  reset to this lsn rounded up to be divisible by
                                IB_FILE_BLOCK_SIZE, after which we add
				LOG_BLOCK_HDR_SIZE
@param[in] new_logs_created     true if resetting logs is done at the log
                                creation; false if it is done after archive
				recovery */
void recv_reset_logs(lsn_t lsn, bool new_logs_created);

/** Creates the recovery system. */
void recv_sys_create();

/** Release recovery system mutexes. */
void recv_sys_close();

/** Frees the recovery system memory. */
void recv_sys_mem_free();

/** Inits the recovery system for a recovery operation
@param[in] size                 Available memory in bytes */
void recv_sys_init(ulint size);

/** Reset the state of the recovery system variables. */
void recv_sys_var_init();

void recv_apply_log_recs(std::vector<recv_t *> records);

/** Applies the hashed log records to the page, if the page lsn is less than
the lsn of a log record. This can be called when a buffer page has just been
read in, or also for a page already in the buffer pool.
@param[in] just_read_in         true if the i/o handler calls this for a
                                freshly read page
@param[in,out] block            buffer block */

void recv_apply_records_to_page(bool just_read_in, buf_block_t *block, std::vector<recv_t *> records);

/** Block of log record data, variable size struct, see note. */
struct recv_data_t {
  /** Pointer to the next block or NULL */
  recv_data_t *next;

  /* Note: the log record data is stored physically immediately after this
  struct, max amount RECV_DATA_BLOCK_SIZE bytes of it */
};

/** Stored log record struct */
struct recv_t {
  /** log record type */
  byte type;

  /** log record body length in bytes */
  ulint len;

  /** space id */
  space_id_t space_id;

  /** page number */
  page_no_t page_no;

  /** chain of blocks containing the log record body */
  recv_data_t *data;

  /** start lsn of the log segment written by the mtr
  which generated this log record: NOTE that this is
  not necessarily the start lsn of this log record */
  lsn_t start_lsn;

  /** end lsn of the log segment written by the mtr
  which generated this log record: NOTE that this is
  not necessarily the end lsn of this log record */
  lsn_t end_lsn;
};

/** Recovery system data structure */
struct recv_sys_t {
  /** mutex protecting the fields apply_log_recs, n_addrs, and
  the state field in each recv_addr struct */
  mutex_t mutex;

  /** log sequence number */
  lsn_t lsn;

  /** size of the log buffer when the database last time wrote to the log */
  ulint last_log_buf_size;

  /** possible incomplete last recovered log block */
  byte *last_block;

  /** the nonaligned start address of the preceding buffer */
  byte *last_block_buf_start;

  /** buffer for parsing log records */
  byte *buf;

  /** amount of data in buf */
  ulint len;

  /** this is the lsn from which we were able to start parsing log
  records and adding them to the hash table; zero if a suitable
  start point not found yet */
  lsn_t parse_start_lsn;

  /** the log data has been scanned up to this lsn */
  lsn_t scanned_lsn;

  /** the log data has been scanned up to this checkpoint number (lowest 4 bytes) */
  ulint scanned_checkpoint_no;

  /** start offset of non-parsed log records in buf */
  ulint recovered_offset;

  /** the log records have been parsed up to this lsn */
  lsn_t recovered_lsn;

  /** recovery should be made at most up to this lsn */
  lsn_t limit_lsn;

  /** this is set to true if we during log scan find a corrupt log block, or a corrupt
  log record, or there is a log parsing buffer overflow */
  bool found_corrupt_log;

  /** memory heap of log records and file addresses*/
  mem_heap_t *heap;
};

/** The recovery system */
extern recv_sys_t *recv_sys;

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

/** Size of the parsing buffer; it must accommodate RECV_SCAN_SIZE many
times! */
constexpr ulint RECV_PARSING_BUF_SIZE = 2 * 1024 * 1024;

/** Size of block reads when the log groups are scanned forward to do a
roll-forward */
constexpr ulint RECV_SCAN_SIZE = 4 * UNIV_PAGE_SIZE;

// stream reader
class append_only_queue {
 public:
  explicit append_only_queue(std::list<recv_t *> *records_list) : m_records_list(records_list) { ut_a(m_records_list != nullptr); }

  void append(recv_t *record) { m_records_list->push_front(record); }

 private:
  std::list<recv_t *> *m_records_list;
};

class redo_log_stream {
 public:
  struct read_result {
    enum class status {
      Ok,
      MemoryBufferFull,
      NoMoreData,
    };

    recv_t *data;
    status status;
  };

  redo_log_stream(byte *buf, uint64_t start_lsn, uint64_t &contiguous_lsn, uint64_t &group_scanned, log_group_t *group);

  read_result read_next();

  uint64_t get_start_lsn() const;

  uint64_t get_group_scanned_lsn() const;

 private:
  void fetch_next();

 private:
  bool m_finished;
  byte *m_buf;
  uint64_t m_start_lsn;
  uint64_t m_current_lsn;
  uint64_t &m_contiguous_lsn;
  uint64_t &m_group_scanned_lsn;
  log_group_t *m_group;
  ib_recovery_t m_recovery;
  ulint m_available_memory;

 public:
  std::list<recv_t *> m_records;
};

// applicator
class redo_log_applicator {

  struct record_list {
    std::unique_ptr<std::mutex> mtx;
    std::list<recv_t *> records;
  };

 public:
  explicit redo_log_applicator(std::int32_t total_workers);

  std::uint64_t records_applied();
  std::uint64_t records_received();

  void add(recv_t *record);

  bool wait_for_queue_drain(std::chrono::milliseconds timeout_ms = 86400000ms);

  ~redo_log_applicator();

 private:
  void apply(std::vector<recv_t *> chunk);
  void worker_loop(std::stop_source stoken, record_list *record_list, std::size_t batch_size);

 private:
  std::int32_t m_total_workers;
  std::size_t m_batch_size;
  std::vector<record_list> m_queues;
  std::vector<std::jthread> m_workers;
  std::vector<std::stop_source> m_stop_sources;
  std::atomic_uint64_t m_total_applied;
  std::atomic_uint64_t m_total_received;
  std::mutex m_mtx;
  std::condition_variable m_queue_drained;
};