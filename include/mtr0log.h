/****************************************************************************
Copyright (c) 1995, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/mtr0log.h
Mini-transaction logging routines

Created 12/7/1995 Heikki Tuuri
*******************************************************/

#ifndef mtr0log_h
#define mtr0log_h

#include "dict0types.h"
#include "innodb0types.h"
#include "mtr0mtr.h"

/** Writes 1 - 4 bytes to a file page buffered in the buffer pool.
Writes the corresponding log record to the mini-transaction log. */

void mlog_write_ulint(
    byte *ptr,   /*!< in: pointer where to write */
    ulint val,   /*!< in: value to write */
    byte type,   /*!< in: MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES */
    mtr_t *mtr); /*!< in: mini-transaction handle */
/** Writes 8 bytes to a file page buffered in the buffer pool.
Writes the corresponding log record to the mini-transaction log. */

void mlog_write_uint64(byte *ptr,   /*!< in: pointer where to write */
                       uint64_t val,  /*!< in: value to write */
                       mtr_t *mtr); /*!< in: mini-transaction handle */
/** Writes a string to a file page buffered in the buffer pool. Writes the
corresponding log record to the mini-transaction log. */

void mlog_write_string(byte *ptr,       /*!< in: pointer where to write */
                       const byte *str, /*!< in: string to write */
                       ulint len,       /*!< in: string length */
                       mtr_t *mtr);     /*!< in: mini-transaction handle */
/** Logs a write of a string to a file page buffered in the buffer pool.
Writes the corresponding log record to the mini-transaction log. */

void mlog_log_string(byte *ptr,   /*!< in: pointer written to */
                     ulint len,   /*!< in: string length */
                     mtr_t *mtr); /*!< in: mini-transaction handle */
/** Writes initial part of a log record consisting of one-byte item
type and four-byte space and page numbers. */

void mlog_write_initial_log_record(
    const byte *ptr, /*!< in: pointer to (inside) a buffer
                     frame holding the file page where
                     modification is made */
    byte type,       /*!< in: log item type: MLOG_1BYTE, ... */
    mtr_t *mtr);     /*!< in: mini-transaction handle */
/** Writes a log record about an .ibd file create/delete/rename.
@return	new value of log_ptr */
inline byte *mlog_write_initial_log_record_for_file_op(
    ulint type,     /*!< in: MLOG_FILE_CREATE, MLOG_FILE_DELETE, or
                    MLOG_FILE_RENAME */
    ulint space_id, /*!< in: space id, if applicable */
    ulint page_no,  /*!< in: page number (not relevant currently) */
    byte *log_ptr,  /*!< in: pointer to mtr log which has been opened */
    mtr_t *mtr);    /*!< in: mtr */
/** Catenates 1 - 4 bytes to the mtr log. */
inline void mlog_catenate_ulint(
    mtr_t *mtr,  /*!< in: mtr */
    ulint val,   /*!< in: value to write */
    ulint type); /*!< in: MLOG_1BYTE, MLOG_2BYTES, MLOG_4BYTES */
/** Catenates n bytes to the mtr log. */

void mlog_catenate_string(mtr_t *mtr,      /*!< in: mtr */
                          const byte *str, /*!< in: string to write */
                          ulint len);      /*!< in: string length */
/** Catenates a compressed ulint to mlog. */
inline void
mlog_catenate_ulint_compressed(mtr_t *mtr, /*!< in: mtr */
                               ulint val); /*!< in: value to write */
/** Catenates a compressed uint64_t to mlog. */
inline void
mlog_catenate_uint64_compressed(mtr_t *mtr,  /*!< in: mtr */
                                uint64_t val); /*!< in: value to write */

/** Opens a buffer to mlog. It must be closed with mlog_close.
@return	buffer, NULL if log mode MTR_LOG_NONE */
inline byte *mlog_open(mtr_t *mtr,  /*!< in: mtr */
                       ulint size); /*!< in: buffer size in bytes; MUST be
                                    smaller than DYN_ARRAY_DATA_SIZE! */

/** Closes a buffer opened to mlog. */
inline void
mlog_close(mtr_t *mtr, /*!< in: mtr */
           byte *ptr); /*!< in: buffer space from ptr up was not used */

/** Writes the initial part of a log record (3..11 bytes).
If the implementation of this function is changed, all
size parameters to mlog_open() should be adjusted accordingly!
@return	new value of log_ptr */
inline byte *mlog_write_initial_log_record_fast(
    const byte *ptr, /*!< in: pointer to (inside) a buffer
                     frame holding the file page where
                     modification is made */
    byte type,       /*!< in: log item type: MLOG_1BYTE, ... */
    byte *log_ptr,   /*!< in: pointer to mtr log which has
                     been opened */
    mtr_t *mtr);     /*!< in: mtr */

/** Parses an initial log record written by mlog_write_initial_log_record.
@return	parsed record end, NULL if not a complete record */
byte *mlog_parse_initial_log_record(
    byte *ptr,       /*!< in: buffer */
    byte *end_ptr,   /*!< in: buffer end */
    byte *type,      /*!< out: log record type: MLOG_1BYTE, ... */
    ulint *space,    /*!< out: space id */
    ulint *page_no); /*!< out: page number */

/** Parses a log record written by mlog_write_ulint or mlog_write_uint64.
@return	parsed record end, NULL if not a complete record */
byte *mlog_parse_nbytes(
    ulint type,      /*!< in: log record type: MLOG_1BYTE, ... */
    byte *ptr,       /*!< in: buffer */
    byte *end_ptr,   /*!< in: buffer end */
    byte *page);     /*!< in: page where to apply the log record, or NULL */

/** Parses a log record written by mlog_write_string.
@return	parsed record end, NULL if not a complete record */

byte *mlog_parse_string(
    byte *ptr,       /*!< in: buffer */
    byte *end_ptr,   /*!< in: buffer end */
    byte *page);     /*!< in: page where to apply the log record, or NULL */

/** Opens a buffer for mlog, writes the initial log record and,
if needed, the field lengths of an index.  Reserves space
for further log entries.  The log entry must be closed with
mtr_close().
@return	buffer, NULL if log mode MTR_LOG_NONE */

byte *mlog_open_and_write_index(
    mtr_t *mtr,          /*!< in: mtr */
    const byte *rec,     /*!< in: index record or page */
    dict_index_t *index, /*!< in: record descriptor */
    byte type,           /*!< in: log item type */
    ulint size);         /*!< in: requested buffer size in bytes
                         (if 0, calls mlog_close() and returns NULL) */

/** Parses a log record written by mlog_open_and_write_index.
@return	parsed record end, NULL if not a complete record */

byte *mlog_parse_index(byte *ptr,           /*!< in: buffer */
                       const byte *end_ptr, /*!< in: buffer end */
                       bool comp, /*!< in: true=compact record format */
                       dict_index_t **index); /*!< out, own: dummy index */

/* Insert, update, and maybe other functions may use this value to define an
extra mlog buffer size for variable size data */
#define MLOG_BUF_MARGIN 256

#ifndef UNIV_NONINL
#include "mtr0log.ic"
#endif

#endif
