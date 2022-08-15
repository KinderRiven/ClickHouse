cmake_minimum_required(VERSION 3.5.1)

project(cache_connector)

set(SRC_BASE ${PROJECT_SOURCE_DIR})
set (CMAKE_CXX_FLAGS "-O3 -std=c++17")

include_directories(
    ${SRC_BASE}/include
)

set(TEST_SRC
    ${SRC_BASE}/src/HelloConnector.cpp
)

add_library(test STATIC ${TEST_SRC})
