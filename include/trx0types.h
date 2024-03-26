/*****************************************************************************

Copyright (c) 1996, 2010, Innobase Oy. All Rights Reserved.

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

/**************************************************/ /**
 @file include/trx0types.h
 Transaction system global type definitions

 Created 3/26/1996 Heikki Tuuri
 *******************************************************/

#pragma once

#include "ut0byte.h"

/** Prepare trx_t::id for being printed via printf(3) */
#define TRX_ID_PREP_PRINTF(id) id

/** printf(3) format used for printing TRX_ID_PRINTF_PREP() */
#define TRX_ID_FMT "%lX"

/** maximum length that a formatted trx_t::id could take, not including
the terminating NUL character. */
constexpr ulint TRX_ID_MAX_LEN  = 17;

/** Transaction */
struct trx_t;

/** Transaction system */
struct trx_sys_t;

/** Doublewrite information */
struct trx_doublewrite_t;

/** Signal */
struct trx_sig_t;

/** Rollback segment */
struct trx_rseg_t;

/** Transaction undo log */
struct trx_undo_t;

/** Array of undo numbers of undo records being rolled back or purged */
struct trx_undo_arr_t;

/** A cell of trx_undo_arr_t */
struct trx_undo_inf_t;

/** The control structure used in the purge operation */
struct trx_purge_t;

/** Rollback command node in a query graph */
struct roll_node_t;

/** Commit command node in a query graph */
struct commit_node_t;

/** SAVEPOINT command node in a query graph */
struct trx_named_savept_t;

/** Transaction savepoint */
struct trx_savept_t;

/** Rollback contexts */
enum trx_rb_ctx {
   /** No rollback */
  RB_NONE = 0,

  /** Normal rollback */
  RB_NORMAL,

  /** Rolling back an incomplete transaction, in crash recovery, rolling back
  an INSERT that was performed by updating a delete-marked record; if the
  delete-marked record no longer exists in an active read view, it will
  be purged. */
  RB_RECOVERY_PURGE_REC,

 /** Rolling back an incomplete transaction, in crash recovery */
  RB_RECOVERY
};

/** Transaction identifier (DB_TRX_ID, DATA_TRX_ID) */
using trx_id_t = uint64_t;

/** Rollback pointer (DB_ROLL_PTR, DATA_ROLL_PTR) */
using roll_ptr_t = uint64_t;

/** Undo number */
using undo_no_t = uint64_t;

/** Transaction savepoint */
struct trx_savept_t {
   /** Least undo number to undo */
  undo_no_t least_undo_no;
};

/** Transaction system header */
using trx_sysf_t = byte;

/** Rollback segment header */
using trx_rsegf_t = byte;

/** Undo segment header */
using trx_usegf_t = byte;

/** Undo log header */
using trx_ulogf_t = byte;

/** Undo log page header */
using trx_upagef_t = byte;

/** Undo log record */
using trx_undo_rec_t = byte;
