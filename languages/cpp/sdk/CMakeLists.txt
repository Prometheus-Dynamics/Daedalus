cmake_minimum_required(VERSION 3.16)
project(daedalus_cpp LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# The worker SDK: the C codec and loop, with the C and C++ headers.
add_library(daedalus STATIC src/daedalus.c)
target_include_directories(daedalus PUBLIC include)
if(MSVC)
  target_compile_options(daedalus PRIVATE /W3)
else()
  target_compile_options(daedalus PRIVATE -Wall -Wextra -Werror -pedantic)
endif()

# The canonical fixture worker: the thirteen nodes, served over stdin and stdout.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
add_executable(daedalus_cpp_worker ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures/nodes.cpp)
target_link_libraries(daedalus_cpp_worker PRIVATE daedalus)
