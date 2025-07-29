/** Copyright (c) 2024 Sunny Bains. All rights reserved. */

#pragma once

#include "innodb0types.h"
#include "log0config.h"

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

#define LOG_BUFFER_SIZE (srv_config.m_log_buffer_size * UNIV_PAGE_SIZE)
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

UT_LIST_NODE_GETTER_DEFINITION(log_group_t, log_groups);
