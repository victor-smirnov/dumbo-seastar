cmake_minimum_required (VERSION 3.1)
project (Dumbo)

set(VERSION "0.1.0")

set(CMAKE_MODULE_PATH  "${CMAKE_SOURCE_DIR}/memoria/build/lib")

set(SEASTAR_CFLAGS "-DFMT_HEADER_ONLY -DHAVE_XEN -DHAVE_HWLOC -DHAVE_NUMA")
set(SEASTAR_LFLAGS "-static-libgcc -static-libstdc++  -L${CMAKE_SOURCE_DIR}/seastar/build/release -Wl,--whole-archive,-lseastar,--no-whole-archive -g -Wl,--no-as-needed   -fvisibility=hidden -pthread  ")

include(Memoria)

include_directories(memoria/include)
include_directories(include)
include_directories(seastar)
include_directories(seastar/fmt)

add_executable(dumbo src/main.cpp)

set_target_properties(dumbo PROPERTIES COMPILE_FLAGS "-g -std=c++14 ${SEASTAR_CFLAGS}")
set_target_properties(dumbo PROPERTIES LINK_FLAGS "${SEASTAR_LFLAGS}")
target_link_libraries(dumbo Memoria uuid aio boost_program_options boost_system boost_filesystem m boost_unit_test_framework)
target_link_libraries(dumbo boost_thread cryptopp rt gnutls gnutlsxx lz4 protobuf dl unwind xenstore hwloc numa pciaccess xml2 z)
