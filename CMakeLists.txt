cmake_minimum_required (VERSION 3.1)
project (Dumbo)

set(VERSION "0.1.0")

set (CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build, options are: Debug Release RelWithDebInfo MinSizeRel." FORCE)
set (CMAKE_CXX_FLAGS_RELEASE_INIT "-O0")
set (CMAKE_CXX_FLAGS_RELEASE "-O0")

if (NOT DEFINED CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release CACHE FORCE)    
endif()

if (CMAKE_BUILD_TYPE MATCHES Debug)
	set(DUMBO_DEBUG_CFLAGS "-fsanitize=address -fsanitize=undefined -fsanitize=leak -DFMT_HEADER_ONLY -DHAVE_XEN -DHAVE_HWLOC -DHAVE_NUMA -fsanitize=address -fsanitize=leak -fsanitize=undefined -fno-sanitize=vptr -DASAN_ENABLED -DDEBUG -DDEBUG_SHARED_PTR -DDEFAULT_ALLOCATOR -DSEASTAR_THREAD_STACK_GUARDS -DASAN_OPTIONS=new_delete_type_mismatch=0")
	set(DUMBO_DEBUG_LIBS  asan ubsan)
	set(SEASTAR_BUILD_TYPE "debug")
	set(MEMORIA_BUILD_TYPE "release")
else ()
	set(DUMBO_DEBUG_CFLAGS "-DFMT_HEADER_ONLY -DHAVE_XEN -DHAVE_HWLOC -DHAVE_NUMA")
	set(DUMBO_DEBUG_LIBS "")
	set(SEASTAR_BUILD_TYPE "release")
	set(MEMORIA_BUILD_TYPE "debug")
endif()

set(CMAKE_MODULE_PATH  "${CMAKE_SOURCE_DIR}/memoria/build/${MEMORIA_BUILD_TYPE}/lib")

set(SEASTAR_CFLAGS "-Wfatal-errors -Wno-overloaded-virtual -DFMT_HEADER_ONLY -DHAVE_XEN -DHAVE_HWLOC -DHAVE_NUMA ${DUMBO_DEBUG_CFLAGS}") #
set(SEASTAR_LFLAGS "-L${CMAKE_SOURCE_DIR}/seastar/build/${SEASTAR_BUILD_TYPE} -Wl,--whole-archive,-lseastar,--no-whole-archive -g -Wl,--no-as-needed   -fvisibility=hidden -pthread  ")

include(Memoria)

include_directories(memoria/include)
include_directories(include)
include_directories(seastar)
include_directories(seastar/fmt)

add_executable(dumbo src/main.cpp src/statics.cpp)

set_target_properties(dumbo PROPERTIES COMPILE_FLAGS "-g -std=gnu++1y ${SEASTAR_CFLAGS}")
set_target_properties(dumbo PROPERTIES LINK_FLAGS "${SEASTAR_LFLAGS}")
target_link_libraries(dumbo Memoria ${DUMBO_DEBUG_LIBS} uuid aio boost_program_options boost_system boost_filesystem m boost_unit_test_framework)
target_link_libraries(dumbo boost_thread cryptopp rt gnutls gnutlsxx lz4 protobuf dl unwind xenstore hwloc numa pciaccess xml2 z)

#add_executable(dump src/dump.cpp)

#set_target_properties(dump PROPERTIES COMPILE_FLAGS "-g -std=gnu++1y")
#target_link_libraries(dump Memoria uuid)

