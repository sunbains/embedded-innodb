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

# Unit tests, one program per component, see ut0env.h, with correctness
# tests and performance tests (suites named *Perf, see ut0perf.h).
#
#   cmake --build build --target unit       build and run the correctness tests
#   cmake --build build --target perf       build and run the performance tests
#   ctest --test-dir build -L unit          run the correctness tests
#   ctest --test-dir build -R test_lock     run one component's tests
#
# The tests create their databases under UNIT_TEST_TMPDIR ($TMPDIR by default).
# The performance results are appended to UNIT_TEST_PERF_OUT (a CSV file).

add_definitions(-DUNIV_BTR_PRINT -DUNIT_TESTING)

SET(LIBS innodb pthread m)

SET(UNIT_TEST_TMPDIR "$ENV{TMPDIR}" CACHE PATH "Directory for the unit tests' databases")

IF(NOT UNIT_TEST_TMPDIR)
  SET(UNIT_TEST_TMPDIR "/tmp")
ENDIF()

# The scaffolding linked into every test program. An object library: the
# logger symbols it defines are needed by libinnodb.a, after it on the link line.
SET(UNIT_TEST_PERF_OUT "${CMAKE_BINARY_DIR}/perf.csv" CACHE FILEPATH "CSV file of the performance results")

add_library(ut0env OBJECT ut0env.cc ut0perf.cc unit_test_logger.cc)
target_link_libraries(ut0env PUBLIC GTest::gtest ${LIBS})

add_custom_target(unit-build)

# ib_unit_test(<name> [source...]): a gtest program, <name>.cc by default,
# registered with ctest twice: <name> runs its correctness tests under the
# "unit" label, <name>_perf its *Perf suites under the "perf" label, alone.
function(ib_unit_test name)
  SET(sources ${ARGN})

  IF(NOT sources)
    SET(sources ${name}.cc)
  ENDIF()

  add_executable(${name} ${sources})
  target_link_libraries(${name} PRIVATE ut0env GTest::gtest_main GTest::gtest ${LIBS})

  add_test(NAME ${name} COMMAND ${name} --gtest_filter=-*Perf.*)
  set_tests_properties(${name} PROPERTIES
    LABELS unit
    TIMEOUT 1200
    ENVIRONMENT "TMPDIR=${UNIT_TEST_TMPDIR}")

  add_test(NAME ${name}_perf COMMAND ${name} --gtest_filter=*Perf.*)
  set_tests_properties(${name}_perf PROPERTIES
    LABELS perf
    TIMEOUT 3600
    RUN_SERIAL TRUE
    ENVIRONMENT "TMPDIR=${UNIT_TEST_TMPDIR};UT_PERF_OUT=${UNIT_TEST_PERF_OUT}")

  add_dependencies(unit-build ${name})
endfunction()

# Building blocks.
ib_unit_test(dyn0dyn-t)
ib_unit_test(ut0slst-t)
ib_unit_test(test_mach)
ib_unit_test(test_mtr)
ib_unit_test(test_rem)
ib_unit_test(test_page)
ib_unit_test(test_sync)
ib_unit_test(test_mem)
ib_unit_test(test_ut)
ib_unit_test(test_os)
ib_unit_test(test_data)
ib_unit_test(test_wal)

# Components of System, in start-up order.
ib_unit_test(test_aio)
ib_unit_test(test_fil)
ib_unit_test(test_buf)
ib_unit_test(test_log)
ib_unit_test(test_recv)
ib_unit_test(test_fsp)
ib_unit_test(test_trx)
ib_unit_test(test_lock)
ib_unit_test(test_lock_mt)
ib_unit_test(test_lock_mt_db)
ib_unit_test(test_latch)
ib_unit_test(test_btr)
ib_unit_test(test_undo)
ib_unit_test(test_dict)
ib_unit_test(test_dblwr)
ib_unit_test(test_row_upd)
ib_unit_test(test_row_ins)
ib_unit_test(test_row_sel)
ib_unit_test(test_row_undo)
ib_unit_test(test_row_vers)
ib_unit_test(test_row_merge)
ib_unit_test(test_purge)
ib_unit_test(test_pread)
ib_unit_test(test_pars)
ib_unit_test(test_srv)

# The whole: System's life cycle and the C++ API.
ib_unit_test(test_system)
ib_unit_test(test_api)

add_custom_target(unit
  COMMAND ${CMAKE_CTEST_COMMAND} -L unit --output-on-failure -j 8
  DEPENDS unit-build
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  USES_TERMINAL)

add_custom_target(perf
  COMMAND ${CMAKE_CTEST_COMMAND} -L perf --output-on-failure -V
  DEPENDS unit-build
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  USES_TERMINAL)
