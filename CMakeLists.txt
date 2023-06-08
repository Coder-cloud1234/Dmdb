cmake_minimum_required (VERSION 2.8)

project(Dmdb)

#[[
configure_file (
  "${PROJECT_SOURCE_DIR}/config.hpp.in"
  "${PROJECT_SOURCE_DIR}/src/config.hpp"
  )
]]

option(MAKE_TEST "Do test" ON)
option(MAKE_TEST_LOAD_CONFIG "Do load config test" OFF)
option(MAKE_TEST_LOGGER "Do logger test" OFF)
option(MAKE_TEST_IP_INVALID "Do logger test" OFF)
option(MAKE_TEST_EVENT_MANAGER "Do event manger test" ON)

if (MAKE_TEST)
    MESSAGE (STATUS "Test option is enabled")
    #The method config.hpp.in doesn't work, so we use this as a tmp workaround to add a macro
    add_definitions(-DMAKE_TEST)
    set(CMAKE_BUILD_TYPE "Debug")
    set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -pg -ggdb")    
    add_subdirectory (src)
    if (MAKE_TEST_LOAD_CONFIG)
      add_executable(testLoadConfig test/unit_test/test_load_config/TestLoadConfig.cpp)
      target_link_libraries(testLoadConfig Dmdb)
    elseif (MAKE_TEST_LOGGER)
      add_executable(testLogger test/unit_test/test_logger/TestLogger.cpp)
      target_link_libraries(testLogger Dmdb)
    elseif (MAKE_TEST_IP_INVALID)
      add_executable(testIPInvalid test/unit_test/test_ip/TestIPInvalid.cpp)
      target_link_libraries(testIPInvalid Dmdb)
    elseif (MAKE_TEST_EVENT_MANAGER)
      add_executable(testEventManagerServer test/unit_test/test_event_manager/TestEventManagerServer.cpp)
      target_link_libraries(testEventManagerServer Dmdb)
      add_executable(testEventManagerClient test/unit_test/test_event_manager/TestEventManagerClient.cpp)
      add_executable(testDmdbClient test/unit_test/test_event_manager/TestDmdbClient.cpp)
    endif()    
else()
    aux_source_directory(src DIR_SRC)
    add_executable(DmdbServer ${DIR_SRC})
endif()





    