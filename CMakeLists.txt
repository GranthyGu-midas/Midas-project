cmake_minimum_required(VERSION 3.20)
project(midas_project LANGUAGES CXX)

option(MARKETLAB_BUILD_TESTS "Build the exchange tests" OFF)
if(MARKETLAB_BUILD_TESTS)
  enable_testing()
endif()

add_subdirectory(src)
