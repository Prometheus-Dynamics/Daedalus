cmake_minimum_required(VERSION 3.16)
project(daedalus_consumer LANGUAGES CXX)
# The installed package is found by its config file, and the consumer links the Rust library through it.
find_package(daedalus CONFIG REQUIRED)
add_executable(consumer consumer.cpp)
target_link_libraries(consumer PRIVATE daedalus::daedalus)
