/***************************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.
Copyright (c) 2024 Sunny Bains. All rights reserved.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

************************************************************************/

/* Create a log function that doesn't print anything. Set it as the
default InnoDB message logger. This function should not print any
messages to stderr. It should simply startup and shutdown InnoDB. */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#define DATABASE "test"
#define TABLE "t"

/** Just ignore all messages. */
static int null_logger(ib_msg_stream_t, const char *, ...) { return 0; }

int main(int argc, char *argv[]) {
  ib_err_t err;

  (void)argc;
  (void)argv;

  err = ib_init();
  assert(err == DB_SUCCESS);

  test_configure();

  ib_logger_set(null_logger, nullptr);

  err = ib_startup("default");
  assert(err == DB_SUCCESS);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

  return (EXIT_SUCCESS);
}
