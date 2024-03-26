/** Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
Copyright (c) 2009 Oracle. All rights reserved.

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
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#ifdef WIN32
#include <windows.h>
#else
#include <unistd.h> /* For sleep() */
#include <getopt.h> /* For getopt_long() */
#endif

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

#ifndef WIN32
static int use_sys_malloc = 0;

/** Get the runtime options. */
static void get_options(int argc, char *argv[]) {
  int opt;

  struct option longopts[] = {{"use-sys-malloc", required_argument, nullptr, 1},
                              {nullptr, 0, nullptr, 0}};

  while ((opt = getopt_long(argc, argv, "", longopts, nullptr)) != -1) {
    switch (opt) {
    case 1:
      use_sys_malloc = 1;
      break;
    default:
      fprintf(stderr, "usage: %s [--use-sys-malloc ]\n", argv[0]);
      exit(EXIT_FAILURE);
    }
  }
}
#endif

int main(int argc, char *argv[]) {
  int i;

#ifndef WIN32
  get_options(argc, argv);
#endif
  for (i = 0; i < 10; ++i) {
    ib_ulint_t err;

    printf(" *** STARTING INNODB *** \n");

    err = ib_init();
    assert(err == DB_SUCCESS);

    test_configure();

#ifdef WIN32
    Sleep(2);
#else
    if (use_sys_malloc) {
      printf("Using system malloc\n");
      err = ib_cfg_set_bool_on("use_sys_malloc");
    } else {
      err = ib_cfg_set_bool_off("use_sys_malloc");
    }
    assert(err == DB_SUCCESS);

    sleep(2);
#endif

    err = ib_shutdown(IB_SHUTDOWN_NORMAL);
    assert(err == DB_SUCCESS);

    printf(" *** SHUTDOWN OF INNODB COMPLETE *** \n");

    /* Note: We check for whether variables are reset
    to their default values externally. */
#ifdef UNIV_DEBUG_VALGRIND
    VALGRIND_DO_LEAK_CHECK;
#endif
  }

  return (EXIT_SUCCESS);
}
