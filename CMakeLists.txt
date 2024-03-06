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

PROJECT (EMBEDDED_INNODB)

SET(CMAKE_BUILD_TYPE debug)
SET(CMAKE_CXX_STANDARD 23)
SET(CMAKE_CXX_STANDARD_REQUIRED True)

# Set both LIBRARY_OUTPUT_PATH and EXECUTABLE_OUTPUT_PATH
SET (LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/lib)
SET (EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)

# Increment if interfaces have been added, removed or changed
SET(API_VERSION 6)

# Increment if source code has changed, set to zero if current is incremented
SET(API_VERSION_REVISION 0)

# Increment if interfaces have been added, set to zero if interfaces have been
# removed or changed
SET(API_VERSION_AGE 0)

ADD_COMPILE_DEFINITIONS(IB_API_VERSION_CURRENT=${API_VERSION})
ADD_COMPILE_DEFINITIONS(IB_API_VERSION_REVISION=${API_VERSION_REVISION})
ADD_COMPILE_DEFINITIONS(IB_API_VERSION_AGE=${API_VERSION_AGE})
ADD_COMPILE_DEFINITIONS(IB_API_VERSION="${API_VERSION}:${API_VERSION_REVISION}:${API_VERSION_AGE}")

ADD_COMPILE_DEFINITIONS(VERSION="0.1")

# Print out CMake info
MESSAGE(STATUS "CMAKE_GENERATOR: " ${CMAKE_GENERATOR})
MESSAGE(STATUS "CMAKE_SOURCE_DIR: " ${CMAKE_SOURCE_DIR})

# Print out system information
MESSAGE(STATUS "CMAKE_SYSTEM: " ${CMAKE_SYSTEM})
MESSAGE(STATUS "CMAKE_SYSTEM_PROCESSOR: " ${CMAKE_SYSTEM_PROCESSOR})
MESSAGE(STATUS "UNIX: " ${UNIX})

SET(IB_LANG "english")

# Check for programs required to build InnoDB e.g., Bison and Flex
FIND_PROGRAM(FLEX flex)
FIND_PROGRAM(BISON bison)

include(CheckTypeSize)
CHECK_TYPE_SIZE(char SIZEOF_CHAR)
CHECK_TYPE_SIZE("unsigned char" SIZEOF_UCHAR)
CHECK_TYPE_SIZE(short SIZEOF_SHORT)
CHECK_TYPE_SIZE("unsigned short" SIZEOF_USHORT)
CHECK_TYPE_SIZE(int SIZEOF_INT)
CHECK_TYPE_SIZE("unsigned int" SIZEOF_UINT)
CHECK_TYPE_SIZE(long SIZEOF_LONG)
CHECK_TYPE_SIZE("unsigned long" SIZEOF_ULONG)
CHECK_TYPE_SIZE("long long int" SIZEOF_LONG_LONG)
CHECK_TYPE_SIZE("unsigned long long int" SIZEOF_ULONG_LONG)
CHECK_TYPE_SIZE(char* SIZEOF_CHARP)
CHECK_TYPE_SIZE(void* SIZEOF_VOIDP)
CHECK_TYPE_SIZE(off_t SIZEOF_OFF_T)
CHECK_TYPE_SIZE(int8_t HAVE_INT8_T)
CHECK_TYPE_SIZE(uint8_t HAVE_UINT8_T)
CHECK_TYPE_SIZE(int16_t HAVE_INT16_T)
CHECK_TYPE_SIZE(uint16_t HAVE_UINT16_T)
CHECK_TYPE_SIZE(int32_t HAVE_INT32_T)
CHECK_TYPE_SIZE(uint32_t HAVE_UINT32_T)
CHECK_TYPE_SIZE(int64_t HAVE_INT64_T)
CHECK_TYPE_SIZE(uint64_t HAVE_UINT64_T)

include(CheckIncludeFiles)
CHECK_INCLUDE_FILES(alloca.h HAVE_ALLOCA_H)
CHECK_INCLUDE_FILES(assert.h HAVE_ASSERT_H)
CHECK_INCLUDE_FILES(atomic.h HAVE_ATOMIC_H)
CHECK_INCLUDE_FILES(ctype.h  HAVE_CTYPE_H)
CHECK_INCLUDE_FILES(dirent.h HAVE_DIRENT_H)
CHECK_INCLUDE_FILES(errno.h HAVE_ERRNO_H)
CHECK_INCLUDE_FILES(fcntl.h HAVE_FCNTL_H)
CHECK_INCLUDE_FILES(inttypes.h HAVE_INTTYPES_H)
CHECK_INCLUDE_FILES(libintl.h HAVE_LIBINTL_H)
CHECK_INCLUDE_FILES(malloc.h HAVE_MALLOC_H)
CHECK_INCLUDE_FILES(math.h HAVE_MATH_H)
CHECK_INCLUDE_FILES(pthread.h HAVE_PTHREAD_H)
CHECK_INCLUDE_FILES(sched.h HAVE_SCHED_H)
CHECK_INCLUDE_FILES(stdarg.h HAVE_STDARG_H)
CHECK_INCLUDE_FILES(stddef.h HAVE_STDDEF_H)
CHECK_INCLUDE_FILES(stdint.h HAVE_STDINT_H)
CHECK_INCLUDE_FILES(stdio.h HAVE_STDIO_H)
CHECK_INCLUDE_FILES(stdlib.h HAVE_STDLIB_H)
CHECK_INCLUDE_FILES(string.h HAVE_STRING_H)
CHECK_INCLUDE_FILES(strings.h HAVE_STRINGS_H)
CHECK_INCLUDE_FILES(sys/ipc.h sys/HAVE_IPC_H)
CHECK_INCLUDE_FILES(sys/mman.h sys/HAVE_MMAN_H)
CHECK_INCLUDE_FILES(sys/resource.h sys/HAVE_RESOURCE_H)
CHECK_INCLUDE_FILES(sys/shm.h sys/HAVE_SHM_H)
CHECK_INCLUDE_FILES(sys/stat.h sys/HAVE_STAT_H)
CHECK_INCLUDE_FILES(sys/time.h sys/HAVE_TIME_H)
CHECK_INCLUDE_FILES(sys/types.h sys/HAVE_TYPES_H)
CHECK_INCLUDE_FILES(sys/utsname.h sys/HAVE_UTSNAME_H)
CHECK_INCLUDE_FILES(time.h HAVE_TIME_H)
CHECK_INCLUDE_FILES(unistd.h HAVE_UNISTD_H)
CHECK_INCLUDE_FILES(valgrind/memcheck.h HAVE_MEMCHECK_H)
CHECK_INCLUDE_FILES(zlib.h HAVE_ZLIB_H)

include(CheckLibraryExists)
CHECK_LIBRARY_EXISTS(m floor "" HAVE_MATH)
CHECK_LIBRARY_EXISTS(z inflate "" HAVE_ZLIB)
CHECK_LIBRARY_EXISTS(pthread pthread_create "" HAVE_PTHREAD)

Include(CheckFunctionExists)
CHECK_FUNCTION_EXISTS(bcmp HAVE_BCMP)
CHECK_FUNCTION_EXISTS(fcntl HAVE_FCNTL)
CHECK_FUNCTION_EXISTS(finite HAVE_FINITE)
CHECK_FUNCTION_EXISTS(fsync HAVE_FSYNC)
CHECK_FUNCTION_EXISTS(ftruncate HAVE_FTRUNCATE)
CHECK_FUNCTION_EXISTS(getcwd HAVE_GETCWD)
CHECK_FUNCTION_EXISTS(getrusage HAVE_GETRUSAGE)
CHECK_FUNCTION_EXISTS(index HAVE_INDEX)
CHECK_FUNCTION_EXISTS(localtime_r HAVE_LOCALTIME_R)
CHECK_FUNCTION_EXISTS(locking HAVE_LOCKING)
CHECK_FUNCTION_EXISTS(memcpy HAVE_MEMCPY)
CHECK_FUNCTION_EXISTS(memmove HAVE_MEMMOVE)
CHECK_FUNCTION_EXISTS(perror HAVE_PERROR)
CHECK_FUNCTION_EXISTS(pread HAVE_PREAD)
CHECK_FUNCTION_EXISTS(mmap HAVE_MMAP)
CHECK_FUNCTION_EXISTS(getpagesize HAVE_GETPAGESIZE)
CHECK_FUNCTION_EXISTS(pthread_attr_setstacksize HAVE_PTHREAD_ATTR_SETSTACKSIZE)
CHECK_FUNCTION_EXISTS(pthread_setprio HAVE_PTHREAD_SETPRIO)
CHECK_FUNCTION_EXISTS(rename HAVE_RENAME)
CHECK_FUNCTION_EXISTS(rint HAVE_RINT)
CHECK_FUNCTION_EXISTS(shmget HAVE_SHMGET)
CHECK_FUNCTION_EXISTS(shmat HAVE_SHMAT)
CHECK_FUNCTION_EXISTS(shmdt HAVE_SHMDT)
CHECK_FUNCTION_EXISTS(shmctl HAVE_SHMCTL)
CHECK_FUNCTION_EXISTS(sleep HAVE_SLEEP)
CHECK_FUNCTION_EXISTS(snprintf HAVE_SNPRINTF)
CHECK_FUNCTION_EXISTS(stpcpy HAVE_STPCPY)
CHECK_FUNCTION_EXISTS(strcasecmp HAVE_STRCASECMP)
CHECK_FUNCTION_EXISTS(strerror HAVE_STRERROR)
CHECK_FUNCTION_EXISTS(strstr HAVE_STRSTR)
CHECK_FUNCTION_EXISTS(strtoul HAVE_STRTOUL)
CHECK_FUNCTION_EXISTS(tell HAVE_TELL)

# Checks for Solaris 10+ atomic functions
IF(CMAKE_SYSTEM_NAME MATCHES "Solaris")
  CHECK_FUNCTION_EXISTS(atomic_cas_ulong HAVE_ATOMIC_CAS_ULONG)
  CHECK_FUNCTION_EXISTS(atomic_cas_32 HAVE_ATOMIC_CAS_32)
  CHECK_FUNCTION_EXISTS(atomic_cas_64 HAVE_ATOMIC_CAS_64)
  CHECK_FUNCTION_EXISTS(atomic_add_long HAVE_ATOMIC_ADD_LONG)

  IF(DEFINED HAVE_ATOMIC_CAS_ULONG
     AND DEFINED HAVE_ATOMIC_CAS_32
     AND DEFINED HAVE_ATOMIC_CAS_64
     AND DEFINED HAVE_ATOMIC_ADD_LONG)
    SET(HAVE_SOLARIS_ATOMICS TRUE)
  ENDIF(DEFINED HAVE_ATOMIC_CAS_ULONG
        AND DEFINED HAVE_ATOMIC_CAS_32
  AND DEFINED HAVE_ATOMIC_CAS_64
  AND DEFINED HAVE_ATOMIC_ADD_LONG)

ENDIF(CMAKE_SYSTEM_NAME MATCHES "Solaris")

SET(CMAKE_EXTRA_INCLUDE_FILES pthread.h)
CHECK_TYPE_SIZE(pthread_t SIZEOF_PTHREAD_T)
SET(CMAKE_EXTRA_INCLUDE_FILES)

# Include directories under innobase
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/include)

SET(INNODB_SOURCES  btr/btr0btr.cc btr/btr0cur.cc btr/btr0pcur.cc
      buf/buf0buf.cc
      buf/buf0flu.cc buf/buf0lru.cc buf/buf0rea.cc
      data/data0data.cc data/data0type.cc
      dict/dict0boot.cc dict/dict0crea.cc dict/dict0dict.cc
      dict/dict0load.cc dict/dict0mem.cc
      dyn/dyn0dyn.cc
      eval/eval0eval.cc eval/eval0proc.cc
      fil/fil0fil.cc
      fsp/fsp0fsp.cc
      fut/fut0lst.cc
      ha/hash0hash.cc ha/ha0storage.cc
      pars/lexyy.cc pars/pars0grm.cc pars/pars0opt.cc
      pars/pars0pars.cc pars/pars0sym.cc
      lock/lock0lock.cc lock/lock0iter.cc
      log/log0log.cc log/log0recv.cc
      mach/mach0data.cc
      mem/mem0mem.cc
      mtr/mtr0log.cc mtr/mtr0mtr.cc
      os/os0file.cc os/os0proc.cc os/os0sync.cc os/os0thread.cc
      page/page0cur.cc page/page0page.cc
      que/que0que.cc
      read/read0read.cc
      rem/rem0cmp.cc rem/rem0rec.cc
      row/row0ext.cc row/row0ins.cc row/row0merge.cc
      row/row0purge.cc row/row0row.cc row/row0prebuilt.cc
      row/row0sel.cc row/row0uins.cc row/row0umod.cc
      row/row0undo.cc row/row0upd.cc row/row0vers.cc
      srv/srv0srv.cc srv/srv0start.cc
      sync/sync0arr.cc sync/sync0rw.cc sync/sync0sync.cc
      thr/thr0loc.cc
      trx/trx0purge.cc trx/trx0rec.cc
      trx/trx0roll.cc trx/trx0rseg.cc
      trx/trx0sys.cc trx/trx0trx.cc trx/trx0undo.cc
      usr/usr0sess.cc ut/ut0dbg.cc ut/ut0mem.cc
      ut/ut0rnd.cc ut/ut0ut.cc ut/ut0vec.cc ut/ut0list.cc ut/ut0rbt.cc
      ddl/ddl0ddl.cc
      api/api0api.cc api/api0misc.cc api/api0ucode.cc
      api/api0cfg.cc api/api0status.cc api/api0sql.cc)

IF(CMAKE_BUILD_TYPE MATCHES debug)
  ADD_DEFINITIONS(-O0 -g -Wall -DUNIV_DEBUG -Werror)
ELSEIF(CMAKE_BUILD_TYPE MATCHES release)
  ADD_DEFINITIONS(-g -O0)
ENDIF(CMAKE_BUILD_TYPE MATCHES debug)

IF(CMAKE_SYSTEM_NAME MATCHES "Linux")
  ADD_DEFINITIONS(-DUNIV_LINUX)
  # Static build for now
  ADD_LIBRARY(innodb STATIC ${INNODB_SOURCES})
ENDIF(CMAKE_SYSTEM_NAME MATCHES "Linux")

IF(NOT DISABLE_XA)
  MESSAGE(STATUS "XA is enabled")
  ADD_DEFINITIONS("-DWITH_XOPEN")
ELSE(NOT DISABLE_XA)
  MESSAGE(STATUS "XA is disabled")
ENDIF(NOT DISABLE_XA)

SUBDIRS(tests)

CONFIGURE_FILE(  ${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/include/ib0config.h)

# List all install files and use CPack for packaging and installation
SET(CMAKE_INSTALL_PREFIX package)

INSTALL(FILES   COPYING
    COPYING.Google
    COPYING.Percona
    COPYING.Sun_Microsystems
  DESTINATION .)

INSTALL(FILES   innodb.h
  DESTINATION include)

INSTALL(FILES   tests/ib_cfg.cc
    tests/ib_compressed.cc
    tests/ib_cursor.cc
    tests/ib_index.cc
    tests/ib_logger.cc
    tests/ib_search.cc
    tests/ib_status.cc
    tests/ib_test1.cc
    tests/ib_test2.cc
    tests/ib_test3.cc
    tests/ib_test5.cc
    tests/ib_types.cc
    tests/ib_update.cc
    tests/test0aux.cc
    tests/test0aux.h
    tests/README
  DESTINATION examples)

INSTALL(FILES   tests/CMakeLists.examples
  DESTINATION examples
  RENAME CMakeLists.txt)

INSTALL(TARGETS innodb
  RUNTIME DESTINATION bin
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)
