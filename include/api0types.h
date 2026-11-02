/****************************************************************************
Copyright (c) 1996, 2009, Innobase Oy. All Rights Reserved.
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

/** @file include/api0types.h
Types shared by the engine and the client API: column types and attributes,
cursor search and match modes, lock modes, isolation levels and shutdown
modes. */

#pragma once

#include <cstdint>

/** @enum ColType  column types that are supported. */
enum ColType : int {
  /** Character varying length. The column is not padded. */
  IB_VARCHAR =  1,

  /** Fixed length character string. The column is padded to the right. */
  IB_CHAR =  2,

  /** Fixed length binary, similar to IB_CHAR but the column is not padded to the right. */
  IB_BINARY =  3,

  /** Variable length binary */
  IB_VARBINARY =  4,

  /** Binary large object, or a TEXT type */
  IB_BLOB  =  5,

  /** Integer: can be any size from 1 - 8 bytes. If the size is
  1, 2, 4 and 8 bytes then you can use the typed read and write functions. For
  other sizes you will need to use the ib_col_get_value() function and do the
  conversion yourself. */
  IB_INT =  6,

  /** System column, this column can be one of DATA_TRX_ID, DATA_ROLL_PTR or DATA_ROW_ID. */
  IB_SYS =  8,

  /** C (float)  floating point value. */
  IB_FLOAT =  9,

  /** C (double) floating point value. */
  IB_DOUBLE =  10,

  /** Decimal stored as an ASCII string */
  IB_DECIMAL =  11,

  /** Any charset, varying length */
  IB_VARCHAR_ANYCHARSET =  12,

  /** Any charset, fixed length */
  IB_CHAR_ANYCHARSET =  13
};

/** @enum TableFormat InnoDB table format types */
enum TableFormat {
  IB_TBL_UNKNOWN = 0,

  /** Default row format. */
  IB_TBL_V1,
};

/** @enum ColAttr InnoDB column attributes */
enum ColAttr {
  /** No special attributes. */
  IB_COL_NONE = 0,

  /** Column data can't be nullptr. */
  IB_COL_NOT_NULL = 1,

  /** Column is IB_INT and unsigned. */
  IB_COL_UNSIGNED = 2,

  /** Future use, reserved. */
  IB_COL_NOT_USED = 4,

  /** Custom precision type, this is a bit that is ignored by InnoDB and so
   * can be set and queried by users. */
  IB_COL_CUSTOM1 = 8,

  /** Custom precision type, this is a bit that is ignored by InnoDB and so
   * can be set and queried by users. */
  IB_COL_CUSTOM2 = 16,

  /** Custom precision type, this is a bit that is ignored by InnoDB and
   * so can be set and queried by users. */
  IB_COL_CUSTOM3 = 32
};

/* Note: must match lock0types.h */
/** @enum CursorLockMode InnoDB lock modes. */
enum CursorLockMode {
  /** Intention shared, an intention lock should be used to lock tables */
  IB_LOCK_IS = 0,

  /** Intention exclusive, an intention lock should be used to lock tables */
  IB_LOCK_IX,

  /** Shared locks should be used to lock rows */
  IB_LOCK_S,

  /** Exclusive locks should be used to lock rows*/
  IB_LOCK_X,

  /** Future use, reserved */
  IB_LOCK_NOT_USED,

  /** This is used internally to note consistent read */
  IB_LOCK_NONE,

  /** number of lock modes */
  IB_LOCK_NUM = IB_LOCK_NONE
};

/** @enum SearchMode InnoDB cursor search modes for ib_cursor_moveto().
Note: Values must match those found in page0cur.h */
enum SearchMode {
    /** Unsupported. */
    IB_CUR_UNSUPP = 0,

  /** If search key is not found then position the cursor on the row
   * that is greater than the search key */
  IB_CUR_G = 1,

  /** If the search key not found then position the cursor on the row
   * that is greater than or equal to the search key */
  IB_CUR_GE = 2,

  /** If search key is not found then position the cursor on the row
   * that is less than the search key */
  IB_CUR_L = 3,

  /** If search key is not found then position the cursor on the row
   * that is less than or equal to the search key */
  IB_CUR_LE = 4
};

/** @enum MatchMode Various match modes used by ib_cursor_moveto() */
enum MatchMode {
  /** Closest match possible */
  IB_CLOSEST_MATCH,

  /** Search using a complete key value */
  IB_EXACT_MATCH,

  /** Search using a key prefix which must match to rows: the prefix may
   * contain an incomplete field (the last field in prefix may be just a
   * prefix of a fixed length column) */
  IB_EXACT_PREFIX
};

/** @struct ColMeta InnoDB column meta data. */
struct ColMeta {
  /** Type of the column */
  ColType  type;

  /** Column attributes */
  ColAttr  attr;

  /** Length of type in bytes. */
  uint32_t  type_len;

  /** 16 bits of data relevant only to the client. InnoDB doesn't care */
  uint16_t  client_type;

  /** Column charset */
  void **  charset;
};

/* Note: Must be in sync with trx0trx.h */
/** @enum TrxState The transaction state can be queried using the
 * ib_trx_state() function. The InnoDB deadlock monitor can roll back a
 * transaction and users should be prepared for this, especially where there
 * is high contention. The way to determine the state of the transaction is to
 * query it's state and check. */
enum TrxState {
  /** Has not started yet, the transaction has not ben started yet.*/
  IB_TRX_NOT_STARTED,

  /** The transaction is currently active and needs to be either
   * committed or rolled back. */
  IB_TRX_ACTIVE,

  /** Not committed to disk yet */
  IB_TRX_COMMITTED_IN_MEMORY,

  /** Support for 2PC/XA */
  IB_TRX_PREPARED
};

/* Note: Must be in sync with TrxIsolation in trx0types.h */
/** @enum TrxLevel Transaction isolation levels */
enum TrxLevel {
  /** Dirty read: non-locking SELECTs are performed so that we do not
   * look at a possible earlier version of a record; thus they are not
   * 'consistent' reads under this isolation level; otherwise like level 2 */
  IB_TRX_READ_UNCOMMITTED = 0,

  /** Somewhat Oracle-like isolation, except that in range UPDATE and DELETE
   * we must block phantom rows with next-key locks; SELECT ... FOR UPDATE
   * and ...  LOCK IN SHARE MODE only lock the index records, NOT the gaps
   * before them, and thus allow free inserting; each consistent read reads
   * its own snapshot */
  IB_TRX_READ_COMMITTED = 1,

  /** All consistent reads in the same trx read the same snapshot; full next-key
   * locking used in locking reads to block insertions into gaps */
  IB_TRX_REPEATABLE_READ = 2,

  /** All plain SELECTs are converted to LOCK IN SHARE MODE reads */
  IB_TRX_SERIALIZABLE = 3
};

/** @enum ShutdownMode When ib_shutdown() is called InnoDB may take a long
 * time to shutdown because of background tasks e.g., purging deleted records.
 * The following flags allow the user to control the shutdown behavior. */
enum ShutdownMode {
  /** Normal shutdown, do insert buffer merge and purge before complete shutdown. */
  IB_SHUTDOWN_NORMAL,

  /** Do not do a purge and index buffer merge at shutdown. */
  IB_SHUTDOWN_NO_IBUFMERGE_PURGE,

  /** Same as NO_IBUFMERGE_PURGE and in addition do not even flush the buffer
   * pool to data files. No committed transactions are lost */
  IB_SHUTDOWN_NO_BUFPOOL_FLUSH
};

/** Generical InnoDB callback prototype. */
