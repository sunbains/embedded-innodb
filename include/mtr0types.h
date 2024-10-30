/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

#include "dyn0dyn.h"
#include "sync0rw.h"

/* Logging modes for a mini-transaction */

enum mtr_log_mode_t : byte {
  /** Default mode: log all operations modifying disk-based data */
  MTR_LOG_ALL = 21,

  /** Log only operations modifying file space page allocation
   * data (operations in fsp0fsp.* ) */
  MTR_LOG_SPACE = 23,

  /** Log only operations modifying file space page allocation
   * data (operations in fsp0fsp.* ) */
  MTR_LOG_NONE = 22,

  /** Inserts are logged in a shorter form */
  MTR_LOG_SHORT_INSERTS = 24,
};

/**
 * @brief Types for the mlock objects to store in the mtr memo;
 * NOTE that the first 3 values must be RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH
 */
enum mtr_memo_type_t : byte {
  /** Page is fixed in an S-latch */
  MTR_MEMO_PAGE_S_FIX = RW_S_LATCH,

  /** Page is fixed in an X-latch */
  MTR_MEMO_PAGE_X_FIX = RW_X_LATCH,

  /** Page is fixed in no latch */
  MTR_MEMO_BUF_FIX = RW_NO_LATCH,

  /** Page is modified */
  MTR_MEMO_MODIFY = 54,

  /** Page is locked in S-mode */
  MTR_MEMO_S_LOCK = 55,

  /** Page is locked in X-mode */
  MTR_MEMO_X_LOCK = 56,
};

/** @name Log item types
The log items are declared 'byte' so that the compiler can warn if val
and type parameters are switched in a call to mlog_write_ulint. NOTE!
For 1 - 8 bytes, the flag value must give the length also! @{ */
enum mlog_type_t : byte {
  /** Unknown log type. */
  MLOG_UNKNOWN = 0,

  /** @brief one byte is written */
  MLOG_1BYTE = 1,

  /** @brief 2 bytes ... */
  MLOG_2BYTES = 2,

  /** @brief 4 bytes ... */
  MLOG_4BYTES = 4,

  /** @brief 8 bytes ... */
  MLOG_8BYTES = 8,

  /** record insert */
  MLOG_REC_INSERT = 9,

  /** Mark clustered index record deleted */
  MLOG_REC_CLUST_DELETE_MARK = 10,

  /** Mark secondary index record deleted */
  MLOG_REC_SEC_DELETE_MARK = 11,

  /** Update of a record, preserves record field sizes */
  MLOG_REC_UPDATE_IN_PLACE = 13,

  /** Delete a record from a page */
  MLOG_REC_DELETE = 14,

  /** Delete record list end on index page */
  MLOG_LIST_END_DELETE = 15,

  /** Delete record list start on index page */
  MLOG_LIST_START_DELETE = 16,

  /** Copy record list end to a new created index page */
  MLOG_LIST_END_COPY_CREATED = 17,

  /** Reorganize an index page. */
  MLOG_PAGE_REORGANIZE = 18,

  /** Create an index page */
  MLOG_PAGE_CREATE = 19,

  /** Insert entry in an undo log */
  MLOG_UNDO_INSERT = 20,

  /** Erase an undo log page end */
  MLOG_UNDO_ERASE_END = 21,

  /** Initialize a page in an undo log */
  MLOG_UNDO_INIT = 22,

  /** Discard an update undo log header */
  MLOG_UNDO_HDR_DISCARD = 23,

  /** Reuse an insert undo log header */
  MLOG_UNDO_HDR_REUSE = 24,

  /** Create an undo log header */
  MLOG_UNDO_HDR_CREATE = 25,

  /** Mark an index record as the predefined minimum record */
  MLOG_REC_MIN_MARK = 26,

  /** Full contents of a page */
  MLOG_FULL_PAGE = 28,

  /** This means that a file page is taken into use and the prior
   * contents of the page should be ignored: in recovery we must
   * not trust the lsn values stored to the file page */
  MLOG_INIT_FILE_PAGE = 29,

  /** Write a string to a page */
  MLOG_WRITE_STRING = 30,

  /** If a single mtr writes several log records, this
   * log record ends the sequence of these records */
  MLOG_MULTI_REC_END = 31,

  /** Dummy log record used to pad a log block full */
  MLOG_DUMMY_RECORD = 32,

  /** Log record about an .ibd file creation */
  MLOG_FILE_CREATE = 33,

  /** Log record about an .ibd file rename */
  MLOG_FILE_RENAME = 34,

  /** Log record about an .ibd file deletion */
  MLOG_FILE_DELETE = 35,

  /** Biggest value (used in assertions) */
  MLOG_BIGGEST_TYPE = 47,

  /** If the mtr contains only one log record for one page,
  i.e., write_initial_log_record has been called only once, this flag is ORed
  to the type of that first log record */
  MLOG_SINGLE_REC_FLAG = 128,
};

/** @name Flags for MLOG_FILE operations
(stored in the page number parameter, called log_flags in the
functions).  The page number parameter was originally written as 0.
Identifies TEMPORARY TABLE in MLOG_FILE_CREATE, MLOG_FILE_CREATE2 */
constexpr byte MLOG_FILE_FLAG_TEMP = 1;

/** Number of slots in memo */
constexpr ulint MTR_BUF_MEMO_SIZE = 200;

enum mtr_state_t : byte{
  /** State is undefinedd. */
  MTR_UNDEFINED = 0,

  /** Mini-transaction is active. */
  MTR_ACTIVE = 123,

  /** Mini-tranaction is committing flag. */
  MTR_COMMITTING = 64,

  /** Mini-tranaction has committed. */
  MTR_COMMITTED = 76,
};

/** Magic marker for mtr_t to check for corruption when compiled
 * in debug mode. */
constexpr ulint MTR_MAGIC_N = 54551;

/* Type definition of a mini-transaction memo stack slot. */
struct mtr_memo_slot_t {
  /** Type of the stored object (MTR_MEMO_S_LOCK, ...) */
  mtr_memo_type_t m_type;

  /** Pointer to the object */
  void *m_object;
};

/**
* @return text representation of mtr_log_t
*/
inline const char *mlog_type_str(mlog_type_t type) noexcept {
  switch (type) {
    case MLOG_1BYTE:
      return "MLOG_1BYTE";
    case MLOG_2BYTES:
      return "MLOG_2BYTES";
    case MLOG_4BYTES:
      return "MLOG_4BYTES";
    case MLOG_8BYTES:
      return "MLOG_8BYTES";
    case MLOG_REC_INSERT:
      return "MLOG_REC_INSERT";
    case MLOG_REC_CLUST_DELETE_MARK:
      return "MLOG_REC_CLUST_DELETE_MARK";
    case MLOG_REC_SEC_DELETE_MARK:
      return "MLOG_REC_SEC_DELETE_MARK";
    case MLOG_REC_UPDATE_IN_PLACE:
      return "MLOG_REC_UPDATE_IN_PLACE";
    case MLOG_REC_DELETE:
      return "MLOG_REC_DELETE";
    case MLOG_LIST_END_DELETE:
      return "MLOG_LIST_END_DELETE";
    case MLOG_LIST_START_DELETE:
      return "MLOG_LIST_START_DELETE";
    case MLOG_LIST_END_COPY_CREATED:
      return "MLOG_LIST_END_COPY_CREATED";
    case MLOG_PAGE_REORGANIZE:
      return "MLOG_PAGE_REORGANIZE";
    case MLOG_PAGE_CREATE:
      return "MLOG_PAGE_CREATE";
    case MLOG_UNDO_INSERT:
      return "MLOG_UNDO_INSERT";
    case MLOG_UNDO_ERASE_END:
      return "MLOG_UNDO_ERASE_END";
    case MLOG_UNDO_INIT:
      return "MLOG_UNDO_INIT";
    case MLOG_UNDO_HDR_DISCARD:
      return "MLOG_UNDO_HDR_DISCARD";
    case MLOG_UNDO_HDR_REUSE:
      return "MLOG_UNDO_HDR_REUSE";
    case MLOG_UNDO_HDR_CREATE:
      return "MLOG_UNDO_HDR_CREATE";
    case MLOG_REC_MIN_MARK:
      return "MLOG_REC_MIN_MARK";
    case MLOG_FULL_PAGE:
      return "MLOG_FULL_PAGE";
    case MLOG_INIT_FILE_PAGE:
      return "MLOG_INIT_FILE_PAGE";
    case MLOG_WRITE_STRING:
      return "MLOG_WRITE_STRING";
    case MLOG_MULTI_REC_END:
      return "MLOG_MULTI_REC_END";
    case MLOG_DUMMY_RECORD:
      return "MLOG_DUMMY_RECORD";
    case MLOG_FILE_CREATE:
      return "MLOG_FILE_CREATE";
    case MLOG_FILE_RENAME:
      return "MLOG_FILE_RENAME";
    case MLOG_FILE_DELETE:
      return "MLOG_FILE_DELETE";
    default:
      log_fatal("Unknown mlog_type_t: ", (ulint) type);
      return "UNKNOWN";
  }
}
