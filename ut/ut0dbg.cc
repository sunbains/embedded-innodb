/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.
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

/** @file ut/ut0dbg.c
Debug utilities for Innobase.

Created 1/30/1994 Heikki Tuuri
**********************************************************************/

#include "innodb0types.h"

#include <libgen.h>

#include "ut0dbg.h"

#if defined(UNIV_SYNC_DEBUG) || !defined(UT_DBG_USE_ABORT)
/** If this is set to true by ut_dbg_assertion_failed(), all threads
will stop at the next ut_a() or ut_ad(). */
bool ut_dbg_stop_threads = false;
#endif /* UNIV_SYNC_DEBUG || !UT_DBG_USE_ABORT */

#if !defined(UT_DBG_USE_ABORT)
/** A null pointer that will be dereferenced to trigger a memory trap */
ulint *ut_dbg_null_ptr = nullptr;
#endif /* !UT_DBG_USE_ABORT */

void ut_dbg_assertion_failed(const char *expr, const char *file, ulint line) {
  ut_print_timestamp(ib_stream);

  ib_logger(
    ib_stream,
    " Assertion failure in thread %lu in file %s line %lu",
    os_thread_pf(os_thread_get_curr_id()),
    basename((char*)file),
    line
  );

  if (expr) {
    ib_logger(ib_stream, " see: %s\n", expr);
  }

  ib_logger(
    ib_stream,
    " We intentionally generate a memory trap. Please submit a detailed bug report on the Embedded InnoDB GitHub repository: ");
#if defined(UNIV_SYNC_DEBUG) || !defined(UT_DBG_USE_ABORT)
  ut_dbg_stop_threads = true;
#endif /* UNIV_SYNC_DEBUG || !UT_DBG_USE_ABORT */
}

#if defined(UNIV_SYNC_DEBUG) || !defined(UT_DBG_USE_ABORT)
void ut_dbg_stop_thread(const char *file, ulint line) {
  ib_logger(ib_stream, "Thread %lu stopped in file %s line %lu\n", os_thread_pf(os_thread_get_curr_id()), basename((char*)file), line);
  os_thread_sleep(1000000000);
}
#endif /* UNIV_SYNC_DEBUG || !UT_DBG_USE_ABORT */
