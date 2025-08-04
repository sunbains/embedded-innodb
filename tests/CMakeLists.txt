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

# This is the CMakeLists for Embedded InnoDB
PROJECT (TESTS)

SET(LIBS innodb pthread m)

# Set binary output directory to bin/
SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../bin)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../include)
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../)

# The innodb library already includes liburing dependency

ADD_EXECUTABLE(ib_cfg ib_cfg.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_cursor ib_cursor.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_ddl ib_ddl.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_dict ib_dict.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_dict-2 ib_dict-2.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_drop ib_drop.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_index ib_index.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_logger ib_logger.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_recover ib_recover.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_shutdown ib_shutdown.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_status ib_status.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_tablename ib_tablename.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_test1 ib_test1.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_test2 ib_test2.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_test3 ib_test3.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_test5 ib_test5.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_types ib_types.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_update ib_update.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_search ib_search.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_parallel_reader ib_parallel_reader.cc test0aux.cc test_logger.cc)

ADD_EXECUTABLE(ib_deadlock ib_deadlock.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_mt_drv ib_mt_drv.cc ib_mt_base.cc ib_mt_t1.cc ib_mt_t2.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_mt_stress ib_mt_stress.cc test0aux.cc test_logger.cc)
ADD_EXECUTABLE(ib_perf1 ib_perf1.cc test0aux.cc test_logger.cc)

LINK_DIRECTORIES(${EMBEDDED_INNODB})

TARGET_LINK_LIBRARIES(ib_cfg PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_cursor PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_ddl PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_dict PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_dict-2 PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_drop PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_index PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_logger PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_recover PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_shutdown PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_status PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_tablename ${LIBS})
TARGET_LINK_LIBRARIES(ib_test1 PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_test2 PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_test3 PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_test5 PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_types PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_update PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_search PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_parallel_reader PRIVATE ${LIBS})

TARGET_LINK_LIBRARIES(ib_deadlock PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_mt_drv PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_mt_stress PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_perf1 PRIVATE ${LIBS})

# Log_core unit tests - minimal dependencies
SET(CMAKE_CXX_STANDARD 20)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)

ADD_EXECUTABLE(test_log0core_simple
  test_log0core_simple.cc 
  test_logger.cc
)

# Simple Log_core integration test using the public API
TARGET_LINK_LIBRARIES(test_log0core_simple PRIVATE ${LIBS})

# Enable testing
ENABLE_TESTING()

# Add simple Log_core integration test
ADD_TEST(NAME Log_coreSimpleTest COMMAND test_log0core_simple)

# Custom target to run just Log_core tests
ADD_CUSTOM_TARGET(test_logcore
    COMMAND test_log0core_simple
    DEPENDS test_log0core_simple
)

