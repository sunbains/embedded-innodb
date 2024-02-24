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
CMAKE_MINIMUM_REQUIRED(VERSION 3.5 FATAL_ERROR)

PROJECT (TESTS)

IF(DEFINED WIN32)
  ADD_DEFINITIONS("-D__WIN__ -D_WINDOWS -D_CRT_SECURE_NO_DEPRECATE")
ENDIF(DEFINED WIN32)

IF(DEFINED UNIX)
  SET(LIBS innodb pthread z m)
ELSE(DEFINED UNIX)
  SET(LIBS innodb)
ENDIF(DEFINED UNIX)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../include)

ADD_EXECUTABLE(ib_cfg ib_cfg.c test0aux.c)
ADD_EXECUTABLE(ib_compressed ib_compressed.c test0aux.c)
ADD_EXECUTABLE(ib_cursor ib_cursor.c test0aux.c)
ADD_EXECUTABLE(ib_ddl ib_ddl.c test0aux.c)
ADD_EXECUTABLE(ib_dict ib_dict.c test0aux.c)
ADD_EXECUTABLE(ib_drop ib_drop.c test0aux.c)
ADD_EXECUTABLE(ib_index ib_index.c test0aux.c)
ADD_EXECUTABLE(ib_logger ib_logger.c test0aux.c)
ADD_EXECUTABLE(ib_recover ib_recover.c test0aux.c)
ADD_EXECUTABLE(ib_shutdown ib_shutdown.c test0aux.c)
ADD_EXECUTABLE(ib_status ib_status.c test0aux.c)
ADD_EXECUTABLE(ib_tablename ib_tablename.c test0aux.c)
ADD_EXECUTABLE(ib_test1 ib_test1.c test0aux.c)
ADD_EXECUTABLE(ib_test2 ib_test2.c test0aux.c)
ADD_EXECUTABLE(ib_test3 ib_test3.c test0aux.c)
ADD_EXECUTABLE(ib_test5 ib_test5.c test0aux.c)
ADD_EXECUTABLE(ib_types ib_types.c test0aux.c)
ADD_EXECUTABLE(ib_update ib_update.c test0aux.c)
ADD_EXECUTABLE(ib_zip ib_zip.c test0aux.c)
ADD_EXECUTABLE(ib_search ib_search.c test0aux.c)

IF(DEFINED UNIX)
  ADD_EXECUTABLE(ib_deadlock ib_deadlock.c test0aux.c)
  ADD_EXECUTABLE(ib_mt_drv ib_mt_drv.c ib_mt_base.c ib_mt_t1.c ib_mt_t2.c test0aux.c)
  ADD_EXECUTABLE(ib_mt_stress ib_mt_stress.c test0aux.c)
  ADD_EXECUTABLE(ib_perf1 ib_perf1.c test0aux.c)
ENDIF(DEFINED UNIX)

LINK_DIRECTORIES(${EMBEDDED_INNODB})

TARGET_LINK_LIBRARIES(ib_cfg PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_compressed PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_cursor PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_ddl PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_dict PRIVATE ${LIBS})
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
TARGET_LINK_LIBRARIES(ib_zip PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(ib_search PRIVATE ${LIBS})

IF(DEFINED UNIX)
  TARGET_LINK_LIBRARIES(ib_deadlock PRIVATE ${LIBS})
  TARGET_LINK_LIBRARIES(ib_mt_drv PRIVATE ${LIBS})
  TARGET_LINK_LIBRARIES(ib_mt_stress PRIVATE ${LIBS})
  TARGET_LINK_LIBRARIES(ib_perf1 PRIVATE ${LIBS})
ENDIF(DEFINED UNIX)
