# Install script for directory: /home/sunny/dev/embedded-innodb

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "package")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Release")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "0")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set default install directory permissions.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/usr/bin/objdump")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/." TYPE FILE FILES
    "/home/sunny/dev/embedded-innodb/COPYING"
    "/home/sunny/dev/embedded-innodb/COPYING.Google"
    "/home/sunny/dev/embedded-innodb/COPYING.Percona"
    "/home/sunny/dev/embedded-innodb/COPYING.Sun_Microsystems"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include" TYPE FILE FILES "/home/sunny/dev/embedded-innodb/innodb.h")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/examples" TYPE FILE FILES
    "/home/sunny/dev/embedded-innodb/tests/ib_cfg.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_compressed.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_cursor.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_index.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_logger.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_search.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_status.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_test1.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_test2.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_test3.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_test5.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_types.cc"
    "/home/sunny/dev/embedded-innodb/tests/ib_update.cc"
    "/home/sunny/dev/embedded-innodb/tests/test0aux.cc"
    "/home/sunny/dev/embedded-innodb/tests/test0aux.h"
    "/home/sunny/dev/embedded-innodb/tests/README"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/examples" TYPE FILE RENAME "CMakeLists.txt" FILES "/home/sunny/dev/embedded-innodb/tests/CMakeLists.examples")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE STATIC_LIBRARY FILES "/home/sunny/dev/embedded-innodb/lib/libinnodb.a")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/home/sunny/dev/embedded-innodb/unit-tests/cmake_install.cmake")
  include("/home/sunny/dev/embedded-innodb/tests/cmake_install.cmake")

endif()

if(CMAKE_INSTALL_COMPONENT)
  set(CMAKE_INSTALL_MANIFEST "install_manifest_${CMAKE_INSTALL_COMPONENT}.txt")
else()
  set(CMAKE_INSTALL_MANIFEST "install_manifest.txt")
endif()

string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
file(WRITE "/home/sunny/dev/embedded-innodb/${CMAKE_INSTALL_MANIFEST}"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
