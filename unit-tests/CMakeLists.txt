# Copyright (C) 2009 Oracle/Innobase Oy
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

CMAKE_MINIMUM_REQUIRED(VERSION 3.5 FATAL_ERROR)

PROJECT (UNIT_TESTS)

# Add debug definitions
add_definitions(-DUNIV_BTR_PRINT -DUNIT_TESTING)

# Set up libraries
SET(LIBS innodb pthread m uring)

# Set up include directories
INCLUDE_DIRECTORIES(
    ${CMAKE_SOURCE_DIR}/../include
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/../include
)

# Set up dyn0dyn-t test
add_executable(dyn0dyn-t dyn0dyn-t.cc)

# Link against Google Test and InnoDB
target_link_libraries(dyn0dyn-t PRIVATE
    GTest::gtest_main
    GTest::gtest
    ${LIBS}
)

# Add test to CTest
add_test(NAME dyn0dyn-t COMMAND dyn0dyn-t)

# Set up ut0slst-t test
add_executable(ut0slst-t ut0slst-t.cc)

# Link against Google Test and InnoDB
target_link_libraries(ut0slst-t PRIVATE
    GTest::gtest_main
    GTest::gtest
    ${LIBS}
)

# Add test to CTest
add_test(NAME ut0slst-t COMMAND ut0slst-t)

# Set up test_lock test
add_executable(test_lock test_lock.cc unit-test.cc)

# Link test_lock
target_link_libraries(test_lock PRIVATE ${LIBS})

# Add test_lock to CTest
add_test(NAME test_lock COMMAND test_lock)
