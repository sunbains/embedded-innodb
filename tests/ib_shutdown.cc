/***************************************************************************
Copyright (c) 2008 Innobase Oy. All rights reserved.
Copyright (c) 2008 Oracle. All rights reserved.
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
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>
#include <getopt.h>

#include "test0aux.h"

#ifdef UNIV_DEBUG_VALGRIND
#include <valgrind/memcheck.h>
#endif

int main() {
  for (int i = 0; i < 10; ++i) {
    printf(" *** STARTING INNODB *** \n");

    Config config;

    test_configure(config);

    System sys;

    test_startup(sys, config);

    sleep(2);

    auto err = sys.shutdown(IB_SHUTDOWN_NORMAL);
    assert(err == DB_SUCCESS);

    printf(" *** SHUTDOWN OF INNODB COMPLETE *** \n");

#ifdef UNIV_DEBUG_VALGRIND
    VALGRIND_DO_LEAK_CHECK;
#endif
  }

  return EXIT_SUCCESS;
}
