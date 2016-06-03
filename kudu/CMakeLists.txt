cmake_minimum_required(VERSION 3.4.2)
PROJECT(kudu)
subdirs(src)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
find_package(kuduClient REQUIRED)
include_directories(SYSTEM ${KUDUCLIENT_INCLUDE_DIR})
