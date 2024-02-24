/***********************************************************************
Copyright (c) 2009 Innobase Oy. All rights reserved.
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
#include <stdio.h>
#include <assert.h>

#include "innodb.h"

#include "test0aux.h"

static void
get_all(void)
{
	const char** var_names;
	ib_u32_t num_status_var;

	OK(ib_status_get_all(&var_names, &num_status_var));

	assert(num_status_var > 1);

	const char**	ptr;
	ib_i64_t	val;

	for (ptr = var_names; *ptr ; ++ptr) {
		ib_err_t	err;

		err = ib_status_get_i64(*ptr, &val);
		assert(err == DB_SUCCESS);

		printf("%s: %d\n", *ptr, (int) val);
	}
}

int
main(int argc, char** argv)
{
	ib_err_t	err;

	(void)argc;
	(void)argv;

	err = ib_init();
	assert(err == DB_SUCCESS);

	test_configure();

	err = ib_startup("barracuda");
	assert(err == DB_SUCCESS);

	get_all();

	err = ib_shutdown(IB_SHUTDOWN_NORMAL);
	assert(err == DB_SUCCESS);

	return(0);
}
