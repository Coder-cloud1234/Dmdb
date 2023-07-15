cmake_minimum_required (VERSION 2.8)

project(Dmdb)

option(MAKE_TEST "Do test" OFF)

if (MAKE_TEST)
    MESSAGE (STATUS "Debug version")
    add_definitions(-DMAKE_TEST)
    set(CMAKE_BUILD_TYPE "Debug")
    set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -pg -ggdb")    
endif()   

aux_source_directory(src DIR_SRC)
add_executable(DmdbServer ${DIR_SRC})






    