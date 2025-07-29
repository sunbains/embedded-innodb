/****************************************************************************
Copyright (c) 2025 Sunny Bains. All rights reserved.

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

/** @file include/log0config.h
Log configuration constants - shared between main code and tests

This file contains all log-related constants to avoid duplication
between main implementation and test code.
*******************************************************/

#pragma once

#include "innodb0types.h"

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
/*@} */
