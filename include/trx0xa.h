/*****************************************************************************
Copyright (c) 1995, 2010, Innobase Oy. All Rights Reserved.
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

#pragma once

/*
 * Transaction branch identification: XID and NULLXID:
 */

/** Sizes of transaction identifier @{ */
/** Maximum size of a transaction identifier, in bytes */
constexpr ulint XIDDATASIZE = 128;

/** Maximum size in bytes of gtrid */
constexpr ulint MAXGTRIDSIZE = 64;

/* }@ */

/** Maximum size in bytes of bqual */
constexpr ulint MAXBQUALSIZE = 64;

/** X/Open XA distributed transaction identifier */
struct XID {
  /** Format identifier; -1 means that the XID is null */
  int64_t formatID;

  /** Value from 1 through 64 */
  uint8_t gtrid_length;

  /** Value from 1 through 64 */
  uint8_t bqual_length;

  /** Distributed transaction identifier */
  char data[XIDDATASIZE];
};

/** X/Open XA distributed transaction status codes */
/* @{ */
/** Normal execution */
constexpr ulint XA_OK = 0;

/** Asynchronous operation already outstanding */
constexpr lint XAER_ASYNC = -2;

/** A resource manager error occurred in the transaction  branch */
constexpr lint XAER_RMERR = -3;

/** The XID is not valid */
constexpr lint XAER_NOTA = -4;

/** Invalid arguments were given */
constexpr lint XAER_INVAL = -5;

/** Routine invoked in an improper context */
constexpr lint XAER_PROTO = -6;

/** Resource manager unavailable */
constexpr lint XAER_RMFAIL = -7;

/** The XID already exists */
constexpr lint XAER_DUPID = -8;

/** Resource manager doing work outside transaction */
constexpr lint XAER_OUTSIDE = -9;

/* @} */

