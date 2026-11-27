cmake_minimum_required(VERSION 3.16)
# Rust links the release DLL C runtime on Windows, so every configuration here does too.
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreadedDLL")
project(daedalus_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# The C API is implemented in Rust: the staticlib cargo builds from languages/cpp-sdk. The Rust test
# that drives this build passes its path as DAEDALUS_RUST_LIB.
set(DAEDALUS_RUST_LIB "" CACHE FILEPATH "The staticlib of the Rust C API (libdaedalus_lang_cpp_sdk.a)")
if(NOT DAEDALUS_RUST_LIB OR NOT EXISTS "${DAEDALUS_RUST_LIB}")
  message(FATAL_ERROR "DAEDALUS_RUST_LIB must name the Rust staticlib of the C API")
endif()

# The host and client libraries a consumer links: the shared libraries of core/capi (daedalus.h)
# and languages/client-capi (daedalus_client.h), both built by cargo as cdylibs.
set(DAEDALUS_HOST_LIB "" CACHE FILEPATH "The shared library of the host C API (daedalus_capi)")
set(DAEDALUS_CLIENT_LIB "" CACHE FILEPATH "The shared library of the client C API (daedalus_client_capi)")
foreach(lib IN ITEMS DAEDALUS_HOST_LIB DAEDALUS_CLIENT_LIB)
  if(NOT ${lib} OR NOT EXISTS "${${lib}}")
    message(FATAL_ERROR "${lib} must name the shared library it stands for")
  endif()
endforeach()

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
target_compile_features(daedalus INTERFACE cxx_std_17)
target_include_directories(daedalus INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(daedalus INTERFACE daedalus_rust ${daedalus_platform_libs})

# The host and client libraries, as imported shared libraries. On Windows the import library is the
# library's name with `.lib` added (cargo writes it beside the DLL).
set(daedalus_api_headers
  ${CMAKE_CURRENT_SOURCE_DIR}/../../../core/capi/include
  ${CMAKE_CURRENT_SOURCE_DIR}/../../client-capi/include)
add_library(daedalus_host_lib SHARED IMPORTED)
set_target_properties(daedalus_host_lib PROPERTIES IMPORTED_LOCATION "${DAEDALUS_HOST_LIB}")
add_library(daedalus_client_lib SHARED IMPORTED)
set_target_properties(daedalus_client_lib PROPERTIES IMPORTED_LOCATION "${DAEDALUS_CLIENT_LIB}")
if(WIN32)
  set_target_properties(daedalus_host_lib PROPERTIES IMPORTED_IMPLIB "${DAEDALUS_HOST_LIB}.lib")
  set_target_properties(daedalus_client_lib PROPERTIES IMPORTED_IMPLIB "${DAEDALUS_CLIENT_LIB}.lib")
endif()

# The host: daedalus.h and daedalus_host.hpp, over the host C API.
add_library(daedalus_host INTERFACE)
target_compile_features(daedalus_host INTERFACE cxx_std_17)
target_include_directories(daedalus_host INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include ${daedalus_api_headers})
target_link_libraries(daedalus_host INTERFACE daedalus_host_lib ${daedalus_platform_libs})

# The client: daedalus_client.h and daedalus_client.hpp, over the client C API.
add_library(daedalus_client INTERFACE)
target_compile_features(daedalus_client INTERFACE cxx_std_17)
target_include_directories(daedalus_client INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include ${daedalus_api_headers})
target_link_libraries(daedalus_client INTERFACE daedalus_client_lib ${daedalus_platform_libs})

if(MSVC)
  set(warnings /W3)
else()
  set(warnings -Wall -Wextra -Werror -pedantic)
endif()

# The canonical fixture worker: the thirteen nodes, served over stdin and stdout.
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
add_executable(daedalus_cpp_worker ${CMAKE_CURRENT_SOURCE_DIR}/../fixtures/nodes.cpp)
target_link_libraries(daedalus_cpp_worker PRIVATE daedalus)
target_compile_options(daedalus_cpp_worker PRIVATE ${warnings})

# Install: the headers (the worker's, the host's and the client's) and the libraries into the prefix,
# and a package config that a consumer finds with find_package(daedalus CONFIG). The Rust library is
# an imported target, which install(EXPORT) cannot carry (it would name the library by a file name the
# consumer cannot resolve), so the config file defines the targets with paths relative to the prefix:
#   daedalus::daedalus   the worker's headers, and the libraries they need
#   daedalus::rust       the Rust staticlib
#   daedalus::host       daedalus.h and daedalus_host.hpp, over the host C API library
#   daedalus::client     daedalus_client.h and daedalus_client.hpp, over the client C API library
get_filename_component(daedalus_rust_file "${DAEDALUS_RUST_LIB}" NAME)
get_filename_component(daedalus_host_file "${DAEDALUS_HOST_LIB}" NAME)
get_filename_component(daedalus_client_file "${DAEDALUS_CLIENT_LIB}" NAME)
set(DAEDALUS_RUST_INSTALL_PATH "${CMAKE_INSTALL_LIBDIR}/${daedalus_rust_file}")
set(DAEDALUS_HOST_INSTALL_PATH "${CMAKE_INSTALL_LIBDIR}/${daedalus_host_file}")
set(DAEDALUS_CLIENT_INSTALL_PATH "${CMAKE_INSTALL_LIBDIR}/${daedalus_client_file}")
set(DAEDALUS_INCLUDE_INSTALL_DIR "${CMAKE_INSTALL_INCLUDEDIR}")
# The link line the config file repeats for the installed targets.
set(DAEDALUS_PLATFORM_LINK_LIBS "${daedalus_platform_libs}")

configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/daedalusConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/daedalusConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/daedalus)
install(FILES
  ${CMAKE_CURRENT_SOURCE_DIR}/include/daedalus_worker.h
  ${CMAKE_CURRENT_SOURCE_DIR}/include/daedalus_worker.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/include/daedalus_host.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/include/daedalus_client.hpp
  ${CMAKE_CURRENT_SOURCE_DIR}/../../../core/capi/include/daedalus.h
  ${CMAKE_CURRENT_SOURCE_DIR}/../../client-capi/include/daedalus_client.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(FILES "${DAEDALUS_RUST_LIB}" "${DAEDALUS_HOST_LIB}" "${DAEDALUS_CLIENT_LIB}"
  DESTINATION ${CMAKE_INSTALL_LIBDIR})
if(WIN32)
  # The import libraries of the two DLLs, which a consumer links against.
  install(FILES "${DAEDALUS_HOST_LIB}.lib" "${DAEDALUS_CLIENT_LIB}.lib" DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/daedalusConfig.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/daedalus)
