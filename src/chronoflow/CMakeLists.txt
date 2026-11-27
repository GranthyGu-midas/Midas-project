cmake_minimum_required(VERSION 3.20)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  project(midas_chronoflow LANGUAGES CXX)
endif()
include(../CMakeLists.txt)

add_library(midas_chronoflow STATIC bundle.cpp connection.cpp runner.cpp)
target_link_libraries(midas_chronoflow PUBLIC marketlab_order_link marketlab_runtime)
target_compile_options(midas_chronoflow PRIVATE -Wall -Wextra -Wpedantic)

add_executable(market_chronoflow main.cpp)
target_link_libraries(market_chronoflow PRIVATE midas_chronoflow)
target_compile_options(market_chronoflow PRIVATE -Wall -Wextra -Wpedantic)
