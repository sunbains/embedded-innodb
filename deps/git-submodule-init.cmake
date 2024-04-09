FIND_PACKAGE(Git QUIET)

SET(DEPS_INSTALL_DIR "${CMAKE_SOURCE_DIR}/deps/install")
SET(LIBURING_SRC_DIR "${CMAKE_SOURCE_DIR}/deps/liburing")
SET(THREAD_POOL_SRC_DIR "${CMAKE_SOURCE_DIR}/deps/thread-pool")

IF(GIT_FOUND AND EXISTS "${CMAKE_SOURCE_DIR}/.git")

  # Update submodules as needed
 OPTION(GIT_SUBMODULE "Check submodules during build" ON)

 IF(GIT_SUBMODULE)

   MESSAGE(STATUS "Submodule update")

   EXECUTE_PROCESS(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                   RESULT_VARIABLE GIT_SUBMOD_RESULT)

   IF(NOT GIT_SUBMOD_RESULT EQUAL "0")
     MESSAGE(FATAL_ERROR 
	     "git submodule update --init --recursive failed with "
	     "${GIT_SUBMOD_RESULT}, please checkout submodules")
   ENDIF()

 ENDIF()

ENDIF()

IF(NOT EXISTS "${LIBURING_SRC_DIR}")
	MESSAGE(FATAL_ERROR "Failed to download liburing from GitHub")
ELSE()

  MESSAGE("-- Add external dependency liburing")

  SET(LIBURING_SRC_DIR "${CMAKE_SOURCE_DIR}/deps/liburing")
  SET(LIBURING_INSTALL_DIR "${DEPS_INSTALL_DIR}/liburing")

  SET(LIBURING_LIB_DIR "${LIBURING_INSTALL_DIR}/lib")
  SET(LIBURING_INCLUDE_DIR "${LIBURING_INSTALL_DIR}/include")

  ExternalProject_Add(liburing
    SOURCE_DIR "${CMAKE_SOURCE_DIR}/deps/liburing"
    PREFIX ${DEPS_INSTALL_DIR}
    BUILD_COMMAND make -C ${LIBURING_SRC_DIR} prefix=${LIBURING_INSTALL_DIR}
    CONFIGURE_COMMAND ""
    INSTALL_COMMAND make 
      -C ${LIBURING_SRC_DIR}
      prefix=${LIBURING_INSTALL_DIR}
      includedir=${LIBURING_INCLUDE_DIR}
      libdir=${LIBURING_LIB_DIR}
      libdevdir=${LIBURING_LIB_DIR}
      mandir=${LIBURING_INSTALL_DIR}/man
      install
  )

  LINK_DIRECTORIES(${LIBURING_LIB_DIR})
  INCLUDE_DIRECTORIES(${LIBURING_INCLUDE_DIR})

ENDIF()

IF(NOT EXISTS "${THREAD_POOL_SRC_DIR}")
  MESsAGE(FATAL_ERROR "Failed to download thread pool from GitHub")
ELSE()

  # It's a header only library.
  MESSAGE("-- Add external dependency thread-pool")

  SET(THREAD_POOL_SRC_DIR "${CMAKE_SOURCE_DIR}/deps/thread-pool")
  SET(THREAD_POOL_INCLUDE_DIR "${THREAD_POOL_SRC_DIR}/include")

  INCLUDE_DIRECTORIES(${THREAD_POOL_INCLUDE_DIR})

ENDIF()
