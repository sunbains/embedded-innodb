#pragma once

#include "dyn0dyn.h"
#include "sync0rw.h"

/* Logging modes for a mini-transaction */

 /** Default mode: log all operations modifying disk-based data */
constexpr ulint MTR_LOG_ALL = 21;

/**
 * @brief Logging mode: log no operations
 */
constexpr ulint MTR_LOG_NONE = 22;

/**
 * @brief Log only operations modifying file space page allocation
 * data (operations in fsp0fsp.* )
 */
constexpr ulint MTR_LOG_SPACE = 23;

/**
 * @brief Inserts are logged in a shorter form
 */
constexpr ulint MTR_LOG_SHORT_INSERTS = 24;

/**
 * @brief Types for the mlock objects to store in the mtr memo;
 * NOTE that the first 3 values must be RW_S_LATCH, RW_X_LATCH, RW_NO_LATCH
 */
constexpr auto MTR_MEMO_PAGE_S_FIX = RW_S_LATCH;
constexpr auto MTR_MEMO_PAGE_X_FIX = RW_X_LATCH;
constexpr ulint MTR_MEMO_BUF_FIX = RW_NO_LATCH;
constexpr ulint MTR_MEMO_MODIFY = 54;
constexpr ulint MTR_MEMO_S_LOCK = 55;
constexpr ulint MTR_MEMO_X_LOCK = 56;

/** @name Log item types
The log items are declared 'byte' so that the compiler can warn if val
and type parameters are switched in a call to mlog_write_ulint. NOTE!
For 1 - 8 bytes, the flag value must give the length also! @{ */
enum mlog_type_t : byte {
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
  MLOG_REC_CLUST_DELETE_MARK =  10,

  /** Mark secondary index record deleted */
  MLOG_REC_SEC_DELETE_MARK =  11,

  /** Update of a record, preserves record field sizes */
  MLOG_REC_UPDATE_IN_PLACE =  13,

  /** Delete a record from a page */
  MLOG_REC_DELETE =  14,

  /** Delete record list end on index page */
  MLOG_LIST_END_DELETE =  15,

  /** Delete record list start on index page */
  MLOG_LIST_START_DELETE =  16,

  /** Copy record list end to a new created index page */
  MLOG_LIST_END_COPY_CREATED =  17,

  /** Reorganize an index page in ROW_FORMAT=REDUNDANT */
  MLOG_PAGE_REORGANIZE =  18,

  /** Create an index page */
  MLOG_PAGE_CREATE =  19,

  /** Insert entry in an undo log */
  MLOG_UNDO_INSERT =  20,

  /** Erase an undo log page end */
  MLOG_UNDO_ERASE_END =  21,

  /** Initialize a page in an undo log */
  MLOG_UNDO_INIT =  22,

  /** Discard an update undo log header */
  MLOG_UNDO_HDR_DISCARD =  23,

  /** Reuse an insert undo log header */
  MLOG_UNDO_HDR_REUSE =  24,

  /** Create an undo log header */
  MLOG_UNDO_HDR_CREATE =  25,

  /** Mark an index record as the predefined minimum record */
  MLOG_REC_MIN_MARK =  26,

  /** Full contents of a page */
  MLOG_FULL_PAGE = 	28,

  /** This means that a file page is taken into use and the prior
   * contents of the page should be ignored: in recovery we must
   * not trust the lsn values stored to the file page */
  MLOG_INIT_FILE_PAGE =  29,

  /** Write a string to a page */
  MLOG_WRITE_STRING =  30,

  /** If a single mtr writes several log records, this
   * log record ends the sequence of these records */
  MLOG_MULTI_REC_END =  31,

  /** Dummy log record used to pad a log block full */
  MLOG_DUMMY_RECORD =  32,

  /** Log record about an .ibd file creation */
  MLOG_FILE_CREATE =  33,

  /** Log record about an .ibd file rename */
  MLOG_FILE_RENAME =  34,

  /** Log record about an .ibd file deletion */
  MLOG_FILE_DELETE =  35,

  /** Mark a compact index record as the predefined minimum record */
  MLOG_COMP_REC_MIN_MARK =  36,

  /** Create a compact index page */
  MLOG_COMP_PAGE_CREATE =  37,

  /** Compact record insert */
  MLOG_COMP_REC_INSERT =  38,

  /*!< mark compact clustered index record deleted */
  MLOG_COMP_REC_CLUST_DELETE_MARK = 39,

  /** Mark compact secondary index record deleted; this log
   * record type is redundant, as MLOG_REC_SEC_DELETE_MARK
   * is independent of the record format. */
  MLOG_COMP_REC_SEC_DELETE_MARK =  40,

  /** Update of a compact record, preserves record field sizes */
  MLOG_COMP_REC_UPDATE_IN_PLACE =  41,

  /** Delete a compact record from a page */
  MLOG_COMP_REC_DELETE =  42,

  /** Delete compact record list end on index page */
  MLOG_COMP_LIST_END_DELETE =  43,

  /** Delete compact record list start on index page */
  MLOG_COMP_LIST_START_DELETE =  44,

  /** Copy compact record list end to a new created index page */
  MLOG_COMP_LIST_END_COPY_CREATED =  45,

  /** Reorganize an index page */
  MLOG_COMP_PAGE_REORGANIZE = 46,

  /*!< log record about creating an .ibd file, with format */
  MLOG_FILE_CREATE2 = 47,

#ifdef UNIV_LOG_LSN_DEBUG
  /** Current LSN */
  MLOG_LSN =  88,

  /** Biggest value (used in assertions) */
  MLOG_BIGGEST_TYPE  = 48,
#else
  /** Biggest value (used in assertions) */
  MLOG_BIGGEST_TYPE  = 47,
#endif /* UNIV_LOG_LSN_DEBUG */

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

/** Magic marker for mtr_t to check for corruption when compiled
 * in debug mode. */
constexpr ulint MTR_MAGIC_N = 54551;

#ifdef UNIV_DEBUG
/** Minit-transaction is active. */
constexpr ulint MTR_ACTIVE = 12231;

/** Mini-tranaction is committing flag. */
constexpr ulint MTR_COMMITTING = 56456;

/** Mini-tranaction has committed. */
constexpr ulint MTR_COMMITTED = 34676;
#endif /* UNIV_DEBUG */

/* Type definition of a mini-transaction memo stack slot. */
struct mtr_memo_slot_t {
  /** Type of the stored object (MTR_MEMO_S_LOCK, ...) */
  ulint type;

  /** Pointer to the object */
  void *object;
};

/* Mini-transaction handle and buffer */
struct mtr_t {
#ifdef UNIV_DEBUG
  /** MTR_ACTIVE, MTR_COMMITTING, MTR_COMMITTED */
  ulint state;
#endif /* UNIV_DEBUG */

  /** Memo stack for locks etc. */
  dyn_array_t memo;

  /** Mini-transaction log */
  dyn_array_t log;

  /** True if the mtr made modifications to buffer pool pages */
  bool modifications;

  /** Count of how many page initial log records have been written to the mtr log */
  ulint n_log_recs;

  /** Specifies which operations should be logged; default value MTR_LOG_ALL */
  ulint log_mode;

  /** Start lsn of the possible log entry for this mtr */
  lsn_t start_lsn;

  /** End lsn of the possible log entry for this mtr */
  lsn_t end_lsn;

#ifdef UNIV_DEBUG
  /** For debugging. */
  ulint magic_n;
#endif /* UNIV_DEBUG */
};
