cmake_minimum_required(VERSION 3.16)
# Rust links the release DLL C runtime on Windows, so every configuration here does too.
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreadedDLL")
project(daedalus_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# The C API is implemented in Rust: the staticlib cargo builds from languages/cpp-sdk. The Rust test
# that drives this build passes its path as DAEDALUS_RUST_LIB.
set(DAEDALUS_RUST_LIB "" CACHE FILEPATH "The staticlib of the Rust C API (libdaedalus_lang_cpp_sdk.a)")
if(NOT DAEDALUS_RUST_LIB OR NOT EXISTS "${DAEDALUS_RUST_LIB}")
  message(FATAL_ERROR "DAEDALUS_RUST_LIB must name the Rust staticlib of the C API")
endif()

# The worker SDK: the C header, and the Rust library behind it with what a Rust staticlib needs from
# the platform.
find_package(Threads REQUIRED)
add_library(daedalus_rust STATIC IMPORTED)
set_target_properties(daedalus_rust PROPERTIES IMPORTED_LOCATION "${DAEDALUS_RUST_LIB}")
set(daedalus_platform_libs Threads::Threads ${CMAKE_DL_LIBS})
if(UNIX)
  list(APPEND daedalus_platform_libs m)
elseif(WIN32)
  # What `rustc --print native-static-libs` names for a staticlib there.
  list(APPEND daedalus_platform_libs kernel32 advapi32 ntdll userenv ws2_32 dbghelp bcrypt)
endif()

add_library(daedalus INTERFACE)
target_include_directories(daedalus INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(daedalus INTERFACE daedalus_rust ${daedalus_platform_libs})

if(MSVC)
  set(warnings /W3)
else()
  set(warnings -Wall -Wextra -Werror -pedantic)
endif()

# The canonical fixture worker: the thirteen nodes, served over stdin and stdout.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
add_executable(daedalus_cpp_worker ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures/nodes.cpp)
target_link_libraries(daedalus_cpp_worker PRIVATE daedalus)

# The one native test: the header links, and a value round-trips. The Rust test runs it with ctest.
enable_testing()
add_executable(daedalus_wire_test ${CMAKE_CURRENT_SOURCE_DIR}/tests/wire_test.cpp)
target_link_libraries(daedalus_wire_test PRIVATE daedalus)
target_compile_options(daedalus_wire_test PRIVATE ${warnings})
add_test(NAME wire COMMAND daedalus_wire_test)
