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

#ifndef log0recv_h
#define log0recv_h

#include "innodb0types.h"

#include "api0api.h"
#include "buf0types.h"
#include "hash0hash.h"
#include "log0log.h"
#include "srv0srv.h"
#include "ut0byte.h"

/** Returns true if recovery is currently running.
@return	recv_recovery_on */
inline bool recv_recovery_is_on(void);

#ifdef UNIV_LOG_ARCHIVE
/** Returns true if recovery from backup is currently running.
@return	recv_recovery_from_backup_on */
inline bool recv_recovery_from_backup_is_on(void);
#endif /* UNIV_LOG_ARCHIVE */

/** Applies the hashed log records to the page, if the page lsn is less than the
lsn of a log record. This can be called when a buffer page has just been
read in, or also for a page already in the buffer pool. */

void recv_recover_page_func(bool just_read_in,
                            /*!< in: true if the i/o handler calls
                            this for a freshly read page */
                            buf_block_t *block); /*!< in/out: buffer block */

/** Wrapper for recv_recover_page_func().
Applies the hashed log records to the page, if the page lsn is less than the
lsn of a log record. This can be called when a buffer page has just been
read in, or also for a page already in the buffer pool.
@param jri	in: true if just read in (the i/o handler calls this for
a freshly read page)
@param block	in/out: the buffer block */
#define recv_recover_page(jri, block) recv_recover_page_func(jri, block)

/** Recovers from a checkpoint. When this function returns, the database is able
to start processing of new user transactions, but the function
recv_recovery_from_checkpoint_finish should be called later to complete
the recovery and free the resources used in it.
@return	error code or DB_SUCCESS */

db_err recv_recovery_from_checkpoint_start_func(
    ib_recovery_t recovery, /*!< in: recovery flag */
#ifdef UNIV_LOG_ARCHIVE
    ulint type,                /*!< in: LOG_CHECKPOINT or
                               LOG_ARCHIVE */
    uint64_t limit_lsn,        /*!< in: recover up to this lsn
                                  if possible */
#endif                         /* UNIV_LOG_ARCHIVE */
    uint64_t min_flushed_lsn,  /*!< in: min flushed lsn from
                                  data files */
    uint64_t max_flushed_lsn); /*!< in: max flushed lsn from
                                  data files */

#ifdef UNIV_LOG_ARCHIVE
/** Wrapper for recv_recovery_from_checkpoint_start_func().
Recovers from a checkpoint. When this function returns, the database is able
to start processing of new user transactions, but the function
recv_recovery_from_checkpoint_finish should be called later to complete
the recovery and free the resources used in it.
@param type	in: LOG_CHECKPOINT or LOG_ARCHIVE
@param lim	in: recover up to this log sequence number if possible
@param min	in: minimum flushed log sequence number from data files
@param max	in: maximum flushed log sequence number from data files
@return	error code or DB_SUCCESS */
#define recv_recovery_from_checkpoint_start(recv, type, lim, min, max)         \
  recv_recovery_from_checkpoint_start_func(recv, type, lim, min, max)
#else /* UNIV_LOG_ARCHIVE */
/** Wrapper for recv_recovery_from_checkpoint_start_func().
Recovers from a checkpoint. When this function returns, the database is able
to start processing of new user transactions, but the function
recv_recovery_from_checkpoint_finish should be called later to complete
the recovery and free the resources used in it.
@param type	ignored: LOG_CHECKPOINT or LOG_ARCHIVE
@param lim	ignored: recover up to this log sequence number if possible
@param min	in: minimum flushed log sequence number from data files
@param max	in: maximum flushed log sequence number from data files
@return	error code or DB_SUCCESS */
#define recv_recovery_from_checkpoint_start(recv, type, lim, min, max)         \
  recv_recovery_from_checkpoint_start_func(recv, min, max)
#endif /* UNIV_LOG_ARCHIVE */

/** Completes recovery from a checkpoint. */
void recv_recovery_from_checkpoint_finish(
    ib_recovery_t recovery); /*!< in: recovery flag */

/** Initiates the rollback of active transactions. */
void recv_recovery_rollback_active(void);

/** Scans log from a buffer and stores new log data to the parsing buffer.
Parses and hashes the log records if new data found.  This function will
apply log records automatically when the hash table becomes full.
@return true if limit_lsn has been reached, or not able to scan any
more in this log group */
bool recv_scan_log_recs(
    ib_recovery_t recovery,       /*!< in: recovery flag */
    ulint available_memory,       /*!< in: we let the hash table of recs
                                 to grow to this size, at the maximum */
    bool store_to_hash,           /*!< in: true if the records should be
                                   stored to the hash table; this is set
                                   to false if just debug checking is
                                   needed */
    const byte *buf,              /*!< in: buffer containing a log
                                  segment or garbage */
    ulint len,                    /*!< in: buffer length */
    uint64_t start_lsn,           /*!< in: buffer start lsn */
    uint64_t *contiguous_lsn,     /*!< in/out: it is known that all log
                                     groups contain contiguous log data up
                                     to this lsn */
    uint64_t *group_scanned_lsn); /*!< out: scanning succeeded up to
                                  this lsn */
/** Resets the logs. The contents of log files will be lost! */

void recv_reset_logs(
    uint64_t lsn, /*!< in: reset to this lsn
                     rounded up to be divisible by
                     OS_FILE_LOG_BLOCK_SIZE, after
                     which we add
                     LOG_BLOCK_HDR_SIZE */
#ifdef UNIV_LOG_ARCHIVE
    ulint arch_log_no,      /*!< in: next archived log file number */
#endif                      /* UNIV_LOG_ARCHIVE */
    bool new_logs_created); /*!< in: true if resetting logs
                           is done at the log creation;
                           false if it is done after
                           archive recovery */

/** Creates the recovery system. */
void recv_sys_create(void);

/** Release recovery system mutexes. */
void recv_sys_close(void);

/** Frees the recovery system memory. */
void recv_sys_mem_free(void);

/** Inits the recovery system for a recovery operation. */
void recv_sys_init(
    ulint available_memory); /*!< in: available memory in bytes */

/** Empties the hash table of stored log records, applying them to appropriate
pages. */
void recv_apply_hashed_log_recs();

#ifdef UNIV_LOG_ARCHIVE
/** Recovers from archived log files, and also from log files, if they exist.
@return	error code or DB_SUCCESS */
ulint recv_recovery_from_archive_start(
    uint64_t min_flushed_lsn, /*!< in: min flushed lsn field from the
                                 data files */
    uint64_t limit_lsn,       /*!< in: recover up to this lsn if
                                 possible */
    ulint first_log_no);      /*!< in: number of the first archived
                              log file to use in the recovery; the
                              file will be searched from
                              INNOBASE_LOG_ARCH_DIR specified in
                              server config file */

/** Completes recovery from archive. */
void recv_recovery_from_archive_finish(void);
#endif /* UNIV_LOG_ARCHIVE */

/** Block of log record data */
typedef struct recv_data_struct recv_data_t;
/** Block of log record data */
struct recv_data_struct {
  recv_data_t *next; /*!< pointer to the next block or NULL */
                     /*!< the log record data is stored physically
                     immediately after this struct, max amount
                     RECV_DATA_BLOCK_SIZE bytes of it */
};

/** Stored log record struct */
typedef struct recv_struct recv_t;
/** Stored log record struct */
struct recv_struct {
  byte type;          /*!< log record type */
  ulint len;          /*!< log record body length in bytes */
  recv_data_t *data;  /*!< chain of blocks containing the log record
                      body */
  uint64_t start_lsn; /*!< start lsn of the log segment written by
                       the mtr which generated this log record: NOTE
                       that this is not necessarily the start lsn of
                       this log record */
  uint64_t end_lsn;   /*!< end lsn of the log segment written by
                         the mtr which generated this log record: NOTE
                         that this is not necessarily the end lsn of
                         this log record */
  UT_LIST_NODE_T(recv_t)
  rec_list; /*!< list of log records for this page */
};

/** States of recv_addr_struct */
enum recv_addr_state {
  /** not yet processed */
  RECV_NOT_PROCESSED,
  /** page is being read */
  RECV_BEING_READ,
  /** log records are being applied on the page */
  RECV_BEING_PROCESSED,
  /** log records have been applied on the page, or they have
  been discarded because the tablespace does not exist */
  RECV_PROCESSED
};

/** Hashed page file address struct */
typedef struct recv_addr_struct recv_addr_t;
/** Hashed page file address struct */
struct recv_addr_struct {
  enum recv_addr_state state;
  /*!< recovery state of the page */
  ulint space;   /*!< space id */
  ulint page_no; /*!< page number */
  UT_LIST_BASE_NODE_T(recv_t)
  rec_list;              /*!< list of log records for this page */
  hash_node_t addr_hash; /*!< hash node in the hash bucket chain */
};

/** Recovery system data structure */
typedef struct recv_sys_struct recv_sys_t;
/** Recovery system data structure */
struct recv_sys_struct {
  mutex_t mutex; /*!< mutex protecting the fields apply_log_recs,
                 n_addrs, and the state field in each recv_addr
                 struct */
  bool apply_log_recs;
  /*!< this is true when log rec application to
  pages is allowed; this flag tells the
  i/o-handler if it should do log record
  application */
  bool apply_batch_on;
  /*!< this is true when a log rec application
  batch is running */
  uint64_t lsn; /*!< log sequence number */
  ulint last_log_buf_size;
  /*!< size of the log buffer when the database
  last time wrote to the log */
  byte *last_block;
  /*!< possible incomplete last recovered log
  block */
  byte *last_block_buf_start;
  /*!< the nonaligned start address of the
  preceding buffer */
  byte *buf; /*!< buffer for parsing log records */
  ulint len; /*!< amount of data in buf */
  uint64_t parse_start_lsn;
  /*!< this is the lsn from which we were able to
  start parsing log records and adding them to
  the hash table; zero if a suitable
  start point not found yet */
  uint64_t scanned_lsn;
  /*!< the log data has been scanned up to this
  lsn */
  ulint scanned_checkpoint_no;
  /*!< the log data has been scanned up to this
  checkpoint number (lowest 4 bytes) */
  ulint recovered_offset;
  /*!< start offset of non-parsed log records in
  buf */
  uint64_t recovered_lsn;
  /*!< the log records have been parsed up to
  this lsn */
  uint64_t limit_lsn; /*!< recovery should be made at most
                       up to this lsn */
  bool found_corrupt_log;
  /*!< this is set to true if we during log
  scan find a corrupt log block, or a corrupt
  log record, or there is a log parsing
  buffer overflow */
#ifdef UNIV_LOG_ARCHIVE
  log_group_t *archive_group;
  /*!< in archive recovery: the log group whose
  archive is read */
#endif                     /* !UNIV_LOG_ARCHIVE */
  mem_heap_t *heap;        /*!< memory heap of log records and file
                           addresses*/
  hash_table_t *addr_hash; /*!< hash table of file addresses of pages */
  ulint n_addrs;           /*!< number of not processed hashed file
                           addresses in the hash table */
};

/** The recovery system */
extern recv_sys_t *recv_sys;

/** true when applying redo log records during crash recovery; false
otherwise.  Note that this is false while a background thread is
rolling back incomplete transactions. */
extern bool recv_recovery_on;

/** true when recv_init_crash_recovery() has been called. */
extern bool recv_needed_recovery;

#ifdef UNIV_DEBUG
/** true if writing to the redo log (mtr_commit) is forbidden.
Protected by log_sys->mutex. */
extern bool recv_no_log_write;
#endif /* UNIV_DEBUG */

/** true if buf_page_is_corrupted() should check if the log sequence
number (FIL_PAGE_LSN) is in the future.  Initially false, and set by
recv_recovery_from_checkpoint_start_func(). */
extern bool recv_lsn_checks_on;
extern ib_cb_t recv_pre_rollback_hook;
/** Maximum page number encountered in the redo log */
extern ulint recv_max_parsed_page_no;

/** Size of the parsing buffer; it must accommodate RECV_SCAN_SIZE many
times! */
#define RECV_PARSING_BUF_SIZE (2 * 1024 * 1024)

/** Size of block reads when the log groups are scanned forward to do a
roll-forward */
#define RECV_SCAN_SIZE (4 * UNIV_PAGE_SIZE)

/** This many frames must be left free in the buffer pool when we scan
the log and store the scanned log records in the buffer pool: we will
use these free frames to read in pages when we start applying the
log records to the database. */
extern ulint recv_n_pool_free_frames;

#ifndef UNIV_NONINL
#include "log0recv.ic"
#endif

/** Reset the state of the recovery system variables. */
void recv_sys_var_init();

#endif
