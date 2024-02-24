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

# Set both LIBRARY_OUTPUT_PATH and EXECUTABLE_OUTPUT_PATH
SET (LIBRARY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)
SET (EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)

IF(NOT DEFINED DISABLE-COMPRESSION)
  INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/zlib)
ENDIF(NOT DEFINED DISABLE-COMPRESSION)

# Print out CMake info
MESSAGE(STATUS "CMAKE_GENERATOR: " ${CMAKE_GENERATOR})
MESSAGE(STATUS "CMAKE_SOURCE_DIR: " ${CMAKE_SOURCE_DIR})

# Print out system information
MESSAGE(STATUS "CMAKE_SYSTEM: " ${CMAKE_SYSTEM})
MESSAGE(STATUS "CMAKE_SYSTEM_PROCESSOR: " ${CMAKE_SYSTEM_PROCESSOR})
MESSAGE(STATUS "UNIX: " ${UNIX})
MESSAGE(STATUS "WIN32: " ${WIN32})

MESSAGE(STATUS "WIN64: " ${WIN64})
MESSAGE(STATUS "MSVC: " ${MSVC})

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

SET(INNODB_SOURCES	btr/btr0btr.c btr/btr0cur.c btr/btr0pcur.c
			btr/btr0sea.c buf/buf0buf.c
			buf/buf0flu.c buf/buf0lru.c buf/buf0rea.c
			data/data0data.c data/data0type.c
			dict/dict0boot.c dict/dict0crea.c dict/dict0dict.c
			dict/dict0load.c dict/dict0mem.c
			dyn/dyn0dyn.c
			eval/eval0eval.c eval/eval0proc.c
			fil/fil0fil.c
			fsp/fsp0fsp.c
			fut/fut0fut.c fut/fut0lst.c
			ha/ha0ha.c ha/hash0hash.c ha/ha0storage.c
			ibuf/ibuf0ibuf.c
			pars/lexyy.c pars/pars0grm.c pars/pars0opt.c
			pars/pars0pars.c pars/pars0sym.c
			lock/lock0lock.c lock/lock0iter.c
			log/log0log.c log/log0recv.c
			mach/mach0data.c
			mem/mem0mem.c
			mtr/mtr0log.c mtr/mtr0mtr.c
			os/os0file.c os/os0proc.c os/os0sync.c os/os0thread.c
			page/page0cur.c page/page0page.c
			que/que0que.c
			read/read0read.c
			rem/rem0cmp.c rem/rem0rec.c
			row/row0ext.c row/row0ins.c row/row0merge.c
			row/row0purge.c row/row0row.c row/row0prebuilt.c
			row/row0sel.c row/row0uins.c row/row0umod.c
			row/row0undo.c row/row0upd.c row/row0vers.c
			srv/srv0que.c srv/srv0srv.c srv/srv0start.c
			sync/sync0arr.c sync/sync0rw.c sync/sync0sync.c
			thr/thr0loc.c
			trx/trx0purge.c trx/trx0rec.c
			trx/trx0roll.c trx/trx0rseg.c
			trx/trx0sys.c trx/trx0trx.c trx/trx0undo.c
			usr/usr0sess.c ut/ut0byte.c ut/ut0dbg.c ut/ut0mem.c
			ut/ut0rnd.c ut/ut0ut.c ut/ut0vec.c ut/ut0list.c
			ddl/ddl0ddl.c
			api/api0api.c api/api0misc.c api/api0ucode.c
			api/api0cfg.c api/api0status.c api/api0sql.c)

IF(NOT DEFINED DISABLE-COMPRESSION)
	SET(INNODB_SOURCES	${INNODB_SOURCES}
				buf/buf0buddy.c page/page0zip.c)
ENDIF(NOT DEFINED DISABLE-COMPRESSION)

IF(DEFINED WIN32)

	ADD_DEFINITIONS("-D__WIN__ -D_WINDOWS -D_CRT_SECURE_NO_DEPRECATE")

	# Check for 64 bit platform
	IF(CMAKE_SIZEOF_VOID_P MATCHES 8)
		SET(ARCH "amd64") 
		ADD_DEFINITIONS("-D_WIN64")
	ELSE(CMAKE_SIZEOF_VOID_P MATCHES 8)
		SET(ARCH "x86")
		ADD_DEFINITIONS("-D_WIN32")
		# Enable Windows InterLocked functions on 32-bit Windows
		ADD_DEFINITIONS(-DHAVE_WINDOWS_ATOMICS)
	ENDIF(CMAKE_SIZEOF_VOID_P MATCHES 8)

	SET(CMAKE_EXE_LINKER_FLAGS 
		"${CMAKE_EXE_LINKER_FLAGS} /MAP /MAPINFO:EXPORTS")

	SET(CMAKE_EXE_LINKER_FLAGS
		"${CMAKE_EXE_LINKER_FLAGS} /STACK:1048576")

	# Removing compiler optimizations for innodb/mem/* files
	# on 64-bit Windows due to 64-bit compiler error, See MySQL
	# Bug #19424, #36366, #34297
	IF(ARCH MATCHES "amd64")
		SET_SOURCE_FILES_PROPERTIES(mem/mem0mem.c 
			PROPERTIES COMPILE_FLAGS -Od)
	ENDIF(ARCH MATCHES "amd64")

	# Windows we always build a DLL
	ADD_LIBRARY(innodb SHARED ${INNODB_SOURCES} win/innodb.def)
	IF(NOT DEFINED DISABLE-COMPRESSION)
		ADD_DEPENDENCIES(innodb zlib)
	ENDIF(NOT DEFINED DISABLE-COMPRESSION)
ELSEIF(DEFINED UNIX)
	IF(CMAKE_BUILD_TYPE MATCHES debug)
		ADD_DEFINITIONS(-g -Wall -DUNIV_DEBUG)
	ELSEIF(CMAKE_BUILD_TYPE MATCHES release)
		ADD_DEFINITIONS(-O2)
	ELSE(CMAKE_BUILD_TYPE MATCHES debug)
		ADD_DEFINITIONS(-g -Wall -Werror)
	ENDIF(CMAKE_BUILD_TYPE MATCHES debug)

	IF(CMAKE_SYSTEM_NAME MATCHES "Linux")

		ADD_DEFINITIONS(-DUNIV_LINUX)

	ELSEIF(CMAKE_SYSTEM_NAME MATCHES "Solaris"
		OR CMAKE_SYSTEM_NAME MATCHES "SunOS")

		ADD_DEFINITIONS(-DUNIV_SOLARIS)

	ELSEIF(CMAKE_SYSTEM_NAME MATCHES "aix")

		ADD_DEFINITIONS(-DUNIV_AIX)

	ENDIF(CMAKE_SYSTEM_NAME MATCHES "Linux")
	
	# Static build for now
	ADD_LIBRARY(innodb STATIC ${INNODB_SOURCES})
ENDIF(DEFINED WIN32)

IF(NOT DEFINED DISABLE-XA)
	ADD_DEFINITIONS("-DWITH_XOPEN")
ENDIF(NOT DEFINED DISABLE-XA)

IF(NOT DEFINED DISABLE-COMPRESSION)
	ADD_DEFINITIONS("-DWITH_ZIP")
	SUBDIRS(zlib)
ENDIF(NOT DEFINED DISABLE-COMPRESSION)

SUBDIRS(tests)
SUBDIRS(c++)

CONFIGURE_FILE(	${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake
		${CMAKE_CURRENT_BINARY_DIR}/include/ib0config.h)

# List all install files and use CPack for packaging and installation
SET(CMAKE_INSTALL_PREFIX package)

INSTALL(FILES 	COPYING
		COPYING.Google
		COPYING.Percona
		COPYING.Sun_Microsystems
	DESTINATION .)
INSTALL(FILES 	innodb.h
	DESTINATION include)
INSTALL(FILES 	tests/ib_cfg.c
		tests/ib_compressed.c
		tests/ib_cursor.c
		tests/ib_index.c
		tests/ib_logger.c
		tests/ib_search.c
		tests/ib_status.c
		tests/ib_test1.c
		tests/ib_test2.c
		tests/ib_test3.c
		tests/ib_test5.c
		tests/ib_types.c
		tests/ib_update.c
		tests/test0aux.c
		tests/test0aux.h
		tests/README
	DESTINATION examples)
INSTALL(FILES 	tests/CMakeLists.examples
	DESTINATION examples
	RENAME CMakeLists.txt)
INSTALL(TARGETS innodb
	RUNTIME DESTINATION bin
	LIBRARY DESTINATION lib
	ARCHIVE DESTINATION lib
)
