# Copyright (C) 2025 Sunny Bains
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

# Inherit configuration from parent project - no need to redefine PROJECT()

# Add debug definitions
add_definitions(-DUNIV_BTR_PRINT -DUNIT_TESTING)

# Set up libraries - use parent project's innodb target
SET(LIBS innodb pthread m)

# Include directories are inherited from parent project

# The innodb library already includes liburing dependency

# Set up dyn0dyn-t test
add_executable(dyn0dyn-t dyn0dyn-t.cc unit_test_logger.cc)

# Link against Google Test and InnoDB
target_link_libraries(dyn0dyn-t PRIVATE
    GTest::gtest_main
    GTest::gtest
    ${LIBS}
)

# Add test to CTest
add_test(NAME dyn0dyn-t COMMAND dyn0dyn-t)

# Set up ut0slst-t test
add_executable(ut0slst-t ut0slst-t.cc unit_test_logger.cc)

# Link against Google Test and InnoDB
target_link_libraries(ut0slst-t PRIVATE
    GTest::gtest_main
    GTest::gtest
    ${LIBS}
)

# Add test to CTest
add_test(NAME ut0slst-t COMMAND ut0slst-t)

# Set up test_lock test
add_executable(test_system test_system.cc unit_test_logger.cc)
target_link_libraries(test_system PRIVATE
    GTest::gtest_main
    GTest::gtest
    ${LIBS}
)
add_test(NAME test_system COMMAND test_system)

add_executable(test_mach test_mach.cc unit_test_logger.cc)
target_link_libraries(test_mach PRIVATE
    GTest::gtest_main
    GTest::gtest
    ${LIBS}
)
add_test(NAME test_mach COMMAND test_mach)

add_executable(test_api test_api.cc unit_test_logger.cc)
target_link_libraries(test_api PRIVATE
    GTest::gtest_main
    GTest::gtest
    ${LIBS}
)
add_test(NAME test_api COMMAND test_api)

add_executable(test_wal test_wal.cc unit_test_logger.cc)
target_link_libraries(test_wal PRIVATE
    GTest::gtest_main
    GTest::gtest
    ${LIBS}
)
add_test(NAME test_wal COMMAND test_wal)

add_executable(test_lock test_lock.cc unit-test.cc unit_test_logger.cc)

# Link test_lock
target_link_libraries(test_lock PRIVATE ${LIBS})

# Add test_lock to CTest
add_test(NAME test_lock COMMAND test_lock)
