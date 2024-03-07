/****************************************************************************
Copyright (c) 1994, 2009, Innobase Oy. All Rights Reserved.

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

/** @file include/ut0dbg.h
Debug utilities for Innobase

Created 1/30/1994 Heikki Tuuri
**********************************************************************/

#pragma once

#include <stdlib.h>

#include "os0thread.h"

#if defined(__GNUC__) && (__GNUC__ > 2)
/** Test if an assertion fails.
@param EXPR	assertion expression
@return		nonzero if EXPR holds, zero if not */
#define UT_DBG_FAIL(EXPR) unlikely(!((bool)(EXPR)))
#else
/** This is used to eliminate compiler warnings */
extern ulint ut_dbg_zero;
/** Test if an assertion fails.
@param EXPR	assertion expression
@return		nonzero if EXPR holds, zero if not */
#define UT_DBG_FAIL(EXPR) !((ulint)(EXPR) + ut_dbg_zero)
#endif

#if __STDC_VERSION__ < 199901L
#if __GNUC__ >= 2
#define __func__ __FUNCTION__
#else
#define __func__ "<unknown>"
#endif
#endif

#define UT_DBG_PRINT_FUNC printf("%s\n", __func__)

/* you must #define UT_DBG_ENTER_FUNC_ENABLED to something before
using this macro */
#define UT_DBG_ENTER_FUNC            \
  do {                               \
    if (UT_DBG_ENTER_FUNC_ENABLED) { \
      UT_DBG_PRINT_FUNC;             \
    }                                \
  } while (0)

/** Report a failed assertion. */

void ut_dbg_assertion_failed(
  const char *expr, /*!< in: the failed assertion */
  const char *file, /*!< in: source file containing the assertion */
  ulint line
); /*!< in: line number of the assertion */

#if defined(__GNUC__) && (__GNUC__ > 2)
#define UT_DBG_USE_ABORT
#endif

#ifndef UT_DBG_USE_ABORT
/** A null pointer that will be dereferenced to trigger a memory trap */
extern ulint *ut_dbg_null_ptr;
#endif /* UT_DBG_USE_ABORT */

#if defined(UNIV_SYNC_DEBUG) || !defined(UT_DBG_USE_ABORT)
/** If this is set to true by ut_dbg_assertion_failed(), all threads
will stop at the next ut_a() or ut_ad(). */
extern bool ut_dbg_stop_threads;

/** Stop a thread after assertion failure. */
void ut_dbg_stop_thread(const char *file, ulint line);
#endif /* UNIV_SYNC_DEBUG || !UT_DBG_USE_ABORT */

#ifdef UT_DBG_USE_ABORT

/** Abort the execution. */
#define UT_DBG_PANIC abort()

/** Stop threads (null operation) */
#define UT_DBG_STOP \
  do {              \
  } while (0)

#else /* UT_DBG_USE_ABORT */

/** Abort the execution. */
#define UT_DBG_PANIC      \
  if (*(ut_dbg_null_ptr)) \
  ut_dbg_null_ptr = nullptr

/** Stop threads in ut_a(). */
#define UT_DBG_STOP                                  \
  do                                                 \
    if (unlikely(ut_dbg_stop_threads)) {             \
      ut_dbg_stop_thread(__FILE__, (ulint)__LINE__); \
    }                                                \
  while (0)

#endif /* UT_DBG_USE_ABORT */

/** Abort execution if EXPR does not evaluate to nonzero.
@param EXPR	assertion expression that should hold */
#define ut_a(EXPR)                                               \
  do {                                                           \
    if (UT_DBG_FAIL(EXPR)) {                                     \
      ut_dbg_assertion_failed(#EXPR, __FILE__, (ulint)__LINE__); \
      UT_DBG_PANIC;                                              \
    }                                                            \
    UT_DBG_STOP;                                                 \
  } while (0)

/** Abort execution. */
#define ut_error                                           \
  do {                                                     \
    ut_dbg_assertion_failed(0, __FILE__, (ulint)__LINE__); \
    UT_DBG_PANIC;                                          \
  } while (0)

#ifdef UNIV_DEBUG

/** Debug assertion. Does nothing unless UNIV_DEBUG is defined. */
#define ut_ad(EXPR) ut_a(EXPR)

/** Debug statement. Does nothing unless UNIV_DEBUG is defined. */
#define ut_d(EXPR) \
  do {             \
    EXPR;          \
  } while (0)

#else /* UNIV_DEBUG */

/** Debug assertion. Does nothing unless UNIV_DEBUG is defined. */
#define ut_ad(EXPR)

/** Debug statement. Does nothing unless UNIV_DEBUG is defined. */
#define ut_d(EXPR)

#endif /* UT_DEBUG */

/** Silence warnings about an unused variable by doing a null assignment.
@param A	the unused variable */
#define UT_NOT_USED(A) A = A
