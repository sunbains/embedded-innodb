#pragma once

#include "innodb0types.h"

#include "sync0rw.h"
#include "sync0sync.h"

#ifdef UNIV_DEBUG
/** Flag: write to log file? */
extern bool log_do_write;

/** Flag: enable debug output when writing to the log? */
extern bool log_debug_writes;

#else  /* UNIV_DEBUG */
/** Write to log */
constexpr bool log_do_write = true;
#endif /* UNIV_DEBUG */

/** Wait modes for log_write_up_to @{ */
constexpr ulint LOG_NO_WAIT = 91;
constexpr ulint LOG_WAIT_ONE_GROUP = 92;
constexpr ulint LOG_WAIT_ALL_GROUPS = 93;
/* @} */

/* Values used as flags */
constexpr ulint LOG_FLUSH = 7652559;
constexpr ulint LOG_CHECKPOINT = 78656949;
constexpr ulint LOG_RECOVER = 98887331;

/* The counting of lsn's starts from this value: this must be non-zero */
constexpr auto LOG_START_LSN = lsn_t(16 * IB_FILE_BLOCK_SIZE);

#define LOG_BUFFER_SIZE (srv_log_buffer_size * UNIV_PAGE_SIZE)

/* Offsets of a log block header */

/** block number which must be > 0 and is allowed to wrap around at 2G; the
highest bit is set to 1 if this is the first log block in a log flush write
segment */
constexpr ulint LOG_BLOCK_HDR_NO = 0;

/** Mask used to get the highest bit in the preceding field */
constexpr ulint LOG_BLOCK_FLUSH_BIT_MASK = 0x80000000UL;

/** Number of bytes of log written to this block */
constexpr ulint LOG_BLOCK_HDR_DATA_LEN = 4;

/* offset of the first start of an mtr log record group in this log block,
0 if none; if the value is the same as LOG_BLOCK_HDR_DATA_LEN, it means
that the first rec group has not yet been catenated to this log block, but
if it will, it will start at this offset; an archive recovery can
start parsing the log records starting from this offset in this log block,
if value not 0 */
constexpr ulint LOG_BLOCK_FIRST_REC_GROUP = 6;

/* 4 lower bytes of the value of log_sys->next_checkpoint_no when the
log block was last written to: if the block has not yet been written full,
this value is only updated before a log buffer flush */
constexpr ulint LOG_BLOCK_CHECKPOINT_NO = 8;

/* size of the log block header in bytes */
constexpr ulint LOG_BLOCK_HDR_SIZE = 12;

/* Offsets of a log block trailer from the end of the block */

/** 4 byte checksum of the log block contents. */
constexpr ulint LOG_BLOCK_CHECKSUM = 4;

/** trailer size in bytes */
constexpr ulint LOG_BLOCK_TRL_SIZE = 4;

/** Maximum number of log groups in log_group_struct::checkpoint_buf */
constexpr ulint LOG_MAX_N_GROUPS = 32;

/*@{ Offsets for a checkpoint field */
constexpr ulint LOG_CHECKPOINT_NO = 0;
constexpr ulint LOG_CHECKPOINT_LSN = 8;
constexpr ulint LOG_CHECKPOINT_OFFSET = 16;
constexpr ulint LOG_CHECKPOINT_LOG_BUF_SIZE = 20;
constexpr ulint LOG_CHECKPOINT_UNUSED_LSN = 24;
constexpr ulint LOG_CHECKPOINT_GROUP_ARRAY = 32;
/*@} */

/* For each value smaller than LOG_MAX_N_GROUPS the following 8 bytes: @{*/
constexpr ulint LOG_CHECKPOINT_UNUSED_FILE_NO = 0;
constexpr ulint LOG_CHECKPOINT_UNUSED_OFFSET = 4;
constexpr ulint LOG_CHECKPOINT_ARRAY_END = LOG_CHECKPOINT_GROUP_ARRAY + LOG_MAX_N_GROUPS * 8;
constexpr ulint LOG_CHECKPOINT_CHECKSUM_1 = LOG_CHECKPOINT_ARRAY_END;
constexpr ulint LOG_CHECKPOINT_CHECKSUM_2 = 4 + LOG_CHECKPOINT_ARRAY_END;
constexpr ulint LOG_CHECKPOINT_FSP_FREE_LIMIT = 8 + LOG_CHECKPOINT_ARRAY_END;
/* @} */

/** current fsp free limit in tablespace 0, in units of one megabyte; this information is only used
by ibbackup to decide if it can truncate unused ends of non-auto-extending data files in space 0 */
constexpr ulint LOG_CHECKPOINT_FSP_MAGIC_N = 12 + LOG_CHECKPOINT_ARRAY_END;

/** This magic number tells if the checkpoint contains the above field: the field was added to InnoDB-3.23.50 */
constexpr ulint LOG_CHECKPOINT_SIZE = 16 + LOG_CHECKPOINT_ARRAY_END;

constexpr ulint LOG_CHECKPOINT_FSP_MAGIC_N_VAL = 1441231243;

/* Offsets of a log file header */

/** log group number */
constexpr ulint LOG_GROUP_ID = 0;

/* lsn of the start of data in this log file */
constexpr ulint LOG_FILE_START_LSN = 4;

/* 4-byte archived log file number; this field is only defined in an archived log file */
constexpr ulint LOG_FILE_NO = 12;

constexpr ulint LOG_FILE_WAS_CREATED_BY_HOT_BACKUP = 16;

/** a 32-byte field which contains the string 'ibbackup' and the
creation time if the log file was created by ibbackup --restore;
when the application is started for the first time on the restored
database, it can print helpful info for the user */
constexpr ulint LOG_FILE_ARCH_COMPLETED = IB_FILE_BLOCK_SIZE;

/** This 4-byte field is true when the writing of an archived log file
has been completed; this field is only defined in an archived log file */
constexpr ulint LOG_FILE_END_LSN = IB_FILE_BLOCK_SIZE + 4;

/** LSN where the archived log file at least extends: actually the
archived log file may extend to a later lsn, as long as it is within the
same log block as this lsn; this field is defined only when an archived log
file has been completely written */
constexpr ulint LOG_CHECKPOINT_1 = IB_FILE_BLOCK_SIZE;

/** First checkpoint field in the log header; we write alternately to the
checkpoint fields when we make new checkpoints; this field is only defined
in the first log file of a log group */
constexpr ulint LOG_CHECKPOINT_2 = 3 * IB_FILE_BLOCK_SIZE;

/* Second checkpoint field in the log header */
constexpr ulint LOG_FILE_HDR_SIZE = 4 * IB_FILE_BLOCK_SIZE;

constexpr ulint LOG_GROUP_OK = 301;

constexpr ulint LOG_GROUP_CORRUPTED = 302;

/** Log group consists of a number of log files, each of the same size; a log
group is implemented as a space in the sense of the module fil0fil. */
struct log_group_t {
  /* The following fields are protected by log_sys->mutex */

  /** Log group id */
  ulint id;

  /** Number of files in the group */
  ulint n_files;

  /** Individual log file size in bytes, including the log
  file header */
  ulint file_size;

  /** File space which implements the log group */
  ulint space_id;

  /** LOG_GROUP_OK or LOG_GROUP_CORRUPTED */
  ulint state;

  /** LSN used to fix coordinates within the log group */
  lsn_t lsn;

  /** The offset of the above lsn */
  ulint lsn_offset;

  /** Number of currently pending flush writes for this log group */
  ulint n_pending_writes;

  /** Unaligned buffers */
  byte **file_header_bufs_ptr;

  /** Buffers for each file header in the group */
  byte **file_header_bufs;

  /** Used only in recovery: recovery scan succeeded up to
  this lsn in this log group */
  lsn_t scanned_lsn;

  /** unaligned checkpoint header */
  byte *checkpoint_buf_ptr;

  /** checkpoint header is written from this buffer to the group */
  byte *checkpoint_buf;

  /** list of log groups */
  UT_LIST_NODE_T(log_group_t) log_groups;
};

/** Redo log buffer */
struct log_t {

  /** Padding to prevent other memory update hotspots from residing on
  the same memory cache line */
  byte pad[64];

  /** Log sequence number */
  lsn_t lsn;

  /** First free offset within the log buffer */
  ulint buf_free;

  /** Mutex protecting the log */
  mutex_t mutex;

  /* Unaligned log buffer */
  byte *buf_ptr;

  /** Log buffer */
  byte *buf;

  /** Log buffer size in bytes */
  ulint buf_size;

  /* recommended maximum value of buf_free, after which the buffer is flushed */
  ulint max_buf_free;

#ifdef UNIV_DEBUG

  /** value of buf free when log was last time opened; only in the debug version */
  ulint old_buf_free;

  /** value of lsn when log was last time opened; only in the debug version */
  lsn_t old_lsn;
#endif /* UNIV_DEBUG */

  /** This is set to true when there may be need to flush the log buffer,
  or preflush buffer pool pages, or make a checkpoint; this MUST be true
  when lsn - last_checkpoint_lsn > max_checkpoint_age; this flag is
  peeked at by log_free_check(), which does not reserve the log mutex */
  bool check_flush_or_checkpoint;

  /** Log groups */
  UT_LIST_BASE_NODE_T_EXTERN(log_group_t, log_groups) log_groups;

  /** The fields involved in the log buffer flush @{ */

  /** First offset in the log buffer where the byte content may not exist
  written to file, e.g., the start offset of a log record catenated later;
  this is advanced when a flush operation is completed to all the log groups */
  ulint buf_next_to_write;

  /** First log sequence number not yet written to any log group; for this
  to be advanced, it is enough that the write i/o has been completed for
  any one log group */
  lsn_t written_to_some_lsn;

  /** First log sequence number not yet written to some log group; for this
  to be advanced, it is enough that the write i/o has been completed for all
  log groups.  Note that since InnoDB currently has only one log group therefore
  this value is redundant. Also it is possible that this value falls behind
  the flushed_to_disk_lsn transiently.  It is appropriate to use either
  flushed_to_disk_lsn or write_lsn which are always up-to-date and accurate. */
  lsn_t written_to_all_lsn;

  /** End lsn for the current running write */
  lsn_t write_lsn;

  /** THe data in buffer has been written up to this offset when the current
  write ends: this field will then be copied to buf_next_to_write */
  ulint write_end_offset;

  /** End lsn for the current running write + flush operation */
  lsn_t current_flush_lsn;

  /** How far we have written the log AND flushed to disk */
  lsn_t flushed_to_disk_lsn;

  /** Number of currently pending flushes or writes */
  ulint n_pending_writes;

  /* We separate the write of the log file and the actual fsync()
  or other method to flush it to disk. The names below shhould really
  be 'flush_or_write'! */

  /** This event is in the reset state when a flush or a write is
  running; a thread should wait for this without owning the log
  mutex, but NOTE that to set or reset this event, the thread MUST
  own the log mutex! */
  os_event_t no_flush_event;

  /** During a flush, this is first false and becomes true when one
  log group has been written or flushed */
  bool one_flushed;

  /** This event is reset when the flush or write has not yet completed
  for any log group; e.g., this means that a transaction has been
  committed when this is set; a thread should wait for this without owning
  the log mutex, but NOTE that to set or reset this event, the thread
  MUST own the log mutex! */
  os_event_t one_flushed_event;

  /** Number of log i/os initiated thus far */
  ulint n_log_ios;

  /** Number of log i/o's at the previous printout */
  ulint n_log_ios_old;

  /** When log_print was last time called */
  time_t last_printout_time;
  /* @} */

  /** Fields involved in checkpoints @{ */

  /** Capacity of the log group; if the checkpoint age exceeds this,
  it is a serious error because it is possible we will then overwrite
  log and spoil crash recovery */
  ulint log_group_capacity;

  /** When this recommended value for lsn - buf_pool_get_oldest_modification()
  s exceeded, we start an asynchronous preflush of pool pages */
  ulint max_modified_age_async;

  /** When this recommended value for lsn - buf_pool_get_oldest_modification()
  is exceeded, we start a synchronous preflush of pool pages */
  ulint max_modified_age_sync;

  /** Administrator-specified checkpoint interval in terms of log growth in
  bytes; the interval actually used by the database can be smaller */
  ulint adm_checkpoint_interval;

  /** When this checkpoint age is exceeded we start an asynchronous writing
  of a new checkpoint */
  ulint max_checkpoint_age_async;

  /** This is the maximum allowed value for lsn - last_checkpoint_lsn when a new query step is started */
  ulint max_checkpoint_age;

  /** Next checkpoint number */
  lsn_t next_checkpoint_no;

  /** Latest checkpoint lsn */
  lsn_t last_checkpoint_lsn;

  /** Next checkpoint lsn */
  lsn_t next_checkpoint_lsn;

  /** Number of currently pending checkpoint writes */
  ulint n_pending_checkpoint_writes;

  /** This latch is x-locked when a checkpoint write is running; a thread should wait for this without owning the log mutex */
  rw_lock_t checkpoint_lock;

  /** Unaligned checkpoint header */
  byte *checkpoint_buf_ptr;

  /** Checkpoint header is read to this buffer */
  byte *checkpoint_buf;
};

UT_LIST_NODE_GETTER_DEFINITION(log_group_t, log_groups);