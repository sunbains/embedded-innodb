/***********************************************************************
Copyright (c) 2009 Innobase Oy. All rights reserved.
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
#include <stdio.h>
#include <assert.h>


#include "test0aux.h"

static void get_all(System &sys) {
  const auto var_names = api::status_get_all();

  assert(var_names.size() > 1);

  for (const auto name : var_names) {
    int64_t val;

    OK(api::status_get_i64(sys, name, &val));

    printf("%s: %d\n", name, (int)val);
  }

  int64_t val;

  assert(api::status_get_i64(sys, "no_such_status_variable", &val) == DB_NOT_FOUND);
}

int main(int argc, char **argv) {
  DbErr err;

  (void)argc;
  (void)argv;

  Config config;

  test_configure(config);

  System sys;

  test_startup(sys, config);

  get_all(sys);

  err = sys.shutdown(IB_SHUTDOWN_NORMAL);
  assert(err == DB_SUCCESS);

  return (0);
}
