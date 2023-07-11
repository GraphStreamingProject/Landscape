cmake_minimum_required(VERSION 3.15)
include (FetchContent)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

project(KEdgeConnectivity)

# Make the default build type Release. If user or another
# project sets a different value than use that
if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "Setting build type to default -- Release")
  set(CMAKE_BUILD_TYPE "Release" CACHE
      STRING "Choose the type of build." FORCE)
endif()
message(STATUS "Experiments Build Type ${CMAKE_BUILD_TYPE}")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  message(STATUS "Adding GNU compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  message(STATUS "Adding MSVC compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Wall")
else()
  message(STATUS "${CMAKE_CXX_COMPILER_ID} not recognized, no flags added")
endif()

# Install GraphZeppelin Project
FetchContent_Declare(
  GraphZeppelin

  GIT_REPOSITORY      https://github.com/GraphStreamingProject/GraphZeppelin
  GIT_TAG             main
)

FetchContent_MakeAvailable(GraphZeppelin)
include_directories(include)

###############################################################
########################  Experiments  ########################
###############################################################
# Test k connectivity
add_executable(k_connectivity
  src/k_connected_graph.cpp
  src/k_connectivity.cpp
)
add_dependencies(k_connectivity GraphZeppelin)
target_link_libraries(k_connectivity PRIVATE GraphZeppelin)
