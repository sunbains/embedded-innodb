/***********************************************************************
Copyright (c) 2009 Innobase Oy. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.
Copyright (c) 2010 Stewart Smith
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
#include <stdio.h>
#include <assert.h>

#include "innodb.h"

#include "test0aux.h"

#include <stdarg.h>

void srv_panic(int, char *, ...);

int panic_called = 0;

void db_panic(void *data, int er, char *msg, ...);

void db_panic(void *data, int er, char *msg, ...) {
  va_list ap;
  (void)data;

  va_start(ap, msg);
  fprintf(stderr, "TEST PANIC HANDLER: %d ", er);
  fprintf(stderr, msg, ap);
  fprintf(stderr, "\n");
  panic_called = 1;
  va_end(ap);
}

int main(int argc, char **argv) {
  ib_err_t err;

  (void)argc;
  (void)argv;

  err = ib_init();
  assert(err == DB_SUCCESS);

  test_configure();

  err = ib_startup("default");
  assert(err == DB_SUCCESS);

  fprintf(stderr, "Set panic handler\n");

  ib_set_panic_handler(db_panic);

  fprintf(stderr, "Test panic\n");

  ib_error_inject(1);

  err = ib_shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_CORRUPTION);
  assert(panic_called == 1);

  return (0);
}
