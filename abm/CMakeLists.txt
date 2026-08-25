cmake_minimum_required(VERSION 3.16)
project(measles_abm CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

add_library(measles_core
  src/config.cpp
  src/types.cpp
  src/params.cpp
  src/population.cpp
  src/model.cpp
  src/reporter.cpp
)
target_include_directories(measles_core PUBLIC src)
target_compile_options(measles_core PRIVATE -Wall -Wextra -Wpedantic)

add_executable(measles_abm src/main.cpp)
target_link_libraries(measles_abm PRIVATE measles_core)
target_compile_options(measles_abm PRIVATE -Wall -Wextra -Wpedantic)

enable_testing()
add_executable(measles_tests tests/test_abm.cpp)
target_link_libraries(measles_tests PRIVATE measles_core)
target_compile_options(measles_tests PRIVATE -Wall -Wextra -Wpedantic)
add_test(NAME measles_tests COMMAND measles_tests)
