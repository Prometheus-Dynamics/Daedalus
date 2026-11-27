cmake_minimum_required(VERSION 3.16)
project(daedalus_consumer LANGUAGES CXX)
# A consumer at the oldest standard a compiler may default to (MSVC's is C++14): the package's
# targets carry the standard their headers need, which raises it.
set(CMAKE_CXX_STANDARD 14)
# The installed package is found by its config file, and the consumer links each library through it.
find_package(daedalus CONFIG REQUIRED)
add_executable(consumer consumer.cpp)
target_link_libraries(consumer PRIVATE daedalus::daedalus daedalus::host daedalus::client)
