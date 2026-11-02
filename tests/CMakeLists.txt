# Copyright (C) 2009 Oracle/Innobase Oy
# Copyright (C) 2026 Sunny Bains
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

# The Embedded InnoDB test programs, written against the C++ API in api0api.h.
# The compiler settings and include directories come from the parent project.


# Test programs built from a single source file plus the shared helpers.
SET(IB_TESTS
  bug579934_open_index_by_name_segv
  ib_cursor
  ib_ddl
  ib_deadlock
  ib_dict
  ib_dict-2
  ib_drop
  ib_duplicate_key_name
  ib_index
  ib_mt_stress
  ib_parallel_reader
  ib_perf1
  ib_recover
  ib_search
  ib_shutdown
  ib_status
  ib_table_statistics
  ib_tablename
  ib_test1
  ib_test2
  ib_test3
  ib_test5
  ib_types
  ib_update
  ib_varchar_order
)

FOREACH(test ${IB_TESTS})
  ADD_EXECUTABLE(${test} ${test}.cc test0aux.cc test_logger.cc)
  TARGET_LINK_LIBRARIES(${test} PRIVATE innodb pthread m)
ENDFOREACH()

ADD_EXECUTABLE(ib_mt_drv ib_mt_drv.cc ib_mt_base.cc ib_mt_t1.cc ib_mt_t2.cc test0aux.cc test_logger.cc)
TARGET_LINK_LIBRARIES(ib_mt_drv PRIVATE innodb pthread m)

ADD_CUSTOM_TARGET(ib_tests DEPENDS ${IB_TESTS} ib_mt_drv)

# Startup and shutdown through the redo log.
ADD_EXECUTABLE(test_log0core_simple test_log0core_simple.cc test_logger.cc)
TARGET_LINK_LIBRARIES(test_log0core_simple PRIVATE innodb pthread m)
