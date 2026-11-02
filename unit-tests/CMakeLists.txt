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
SET(MINIMAL_SRC global_vars.cc ut0new.cc ut0dbg.cc ut0ut.cc)

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

add_executable(test_lock test_lock.cc unit-test.cc unit_test_logger.cc)

# Link test_lock
target_link_libraries(test_lock PRIVATE ${LIBS})

# Add test_lock to CTest
add_test(NAME test_lock COMMAND test_lock)

# Set up test_log_io test
add_executable(test_log_io test_log_io.cc unit_test_logger.cc)

# Link against Google Test and InnoDB
target_link_libraries(test_log_io PRIVATE
    GTest::gtest_main
    GTest::gtest
    ${LIBS}
)

# Add test to CTest
add_test(NAME test_log_io COMMAND test_log_io)

# Set up test_log_io_uring test with minimal dependencies
add_executable(test_log_io_uring test_log_io_uring.cc global_vars.cc ../log/log_io_uring.cc ${MINIMAL_SRC})

# Link with minimal dependencies
target_link_libraries(test_log_io_uring PRIVATE pthread uring)

# Add test to CTest
add_test(NAME test_log_io_uring COMMAND test_log_io_uring)

# Set up test_log_buffer test - standalone with minimal dependencies
add_executable(test_log_buffer test_log_buffer.cc)

# Link test_log_buffer with minimal dependencies
target_link_libraries(test_log_buffer PRIVATE pthread)

# Add test_log_buffer to CTest
add_test(NAME test_log_buffer COMMAND test_log_buffer)

# Add test_log_vfile executable
add_executable(test_log_vfile test_log_vfile.cc ../log/log_vfile.cc ${MINIMAL_SRC})

# Link test_log_vfile with minimal dependencies
target_link_libraries(test_log_vfile PRIVATE pthread)

# Add test_log_vfile to CTest
add_test(NAME test_log_vfile COMMAND test_log_vfile)
