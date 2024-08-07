CMAKE_MINIMUM_REQUIRED(VERSION 3.5 FATAL_ERROR)

PROJECT(UNIT_TESTS)

SET(LIBS innodb pthread m uring)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../include)

ADD_DEFINITIONS(-DUNIT_TESTING)

ADD_EXECUTABLE(test_lock test_lock.cc unit-test.cc)
ADD_EXECUTABLE(test_dict_table_lru unit-test.cc test_dict_table_lru.cc)

LINK_DIRECTORIES(${EMBEDDED_INNODB})

TARGET_LINK_LIBRARIES(test_lock PRIVATE ${LIBS})
TARGET_LINK_LIBRARIES(test_dict_table_lru PRIVATE ${LIBS})
