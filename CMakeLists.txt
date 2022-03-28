cmake_minimum_required(VERSION 3.15)
include (FetchContent)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

project(DistributedStreamingCC)

# Make the default build type Release. If user or another
# project sets a different value than use that
if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "Setting build type to default -- Release")
  set(CMAKE_BUILD_TYPE "Release" CACHE
      STRING "Choose the type of build." FORCE)
endif()
message(STATUS "DistributedCC Build Type ${CMAKE_BUILD_TYPE}")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  message(STATUS "Adding GNU compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -W -Wall")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
  message(STATUS "Adding MSVC compiler flags")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /Wall")
else()
  message(STATUS "${CMAKE_CXX_COMPILER_ID} not recognized, no flags added")
endif()

######
# Get MPI for distributed communication
######
find_package(MPI REQUIRED)
include_directories(${MPI_INCLUDE_PATH})
add_definitions(-DOMPI_SKIP_MPICXX)
if(MPI_CXX_FOUND)
       message("Found MPI_CXX")
endif()

# Install GraphStreamingCC Project
FetchContent_Declare(
  GraphStreamingCC

  GIT_REPOSITORY      https://github.com/GraphStreamingProject/GraphStreamingCC
  GIT_TAG             main

)
FetchContent_MakeAvailable(GraphStreamingCC)

# The library for distributing the CPU work for
# generating sketch deltas
add_library(DistribUpdateStreamingCC
  src/worker_cluster.cpp
  src/work_distributor.cpp
  src/distributed_worker.cpp
  src/graph_distrib_update.cpp
)
add_dependencies(DistribUpdateStreamingCC GraphStreamingCC)
target_link_libraries(DistribUpdateStreamingCC PUBLIC GraphStreamingCC ${MPI_LIBRARIES})
target_include_directories(DistribUpdateStreamingCC PUBLIC include/ ${MPI_C_INCLUDE_PATH})
# TODO: Is MPI INCLUDE PATH necessary?
if(MPI_COMPILE_FLAGS)
  set_target_properties(DistribUpdateStreamingCC PROPERTIES
    COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
endif()
if(MPI_LINK_FLAGS)
  set_target_properties(DistribUpdateStreamingCC PROPERTIES
    LINK_FLAGS "${MPI_LINK_FLAGS}")
endif()

# A library for testing our code for distributing
# generating sketch deltas
add_library(DistribUpdateStreamingVerifyCC
  src/worker_cluster.cpp
  src/work_distributor.cpp
  src/distributed_worker.cpp
  src/graph_distrib_update.cpp
)
add_dependencies(DistribUpdateStreamingVerifyCC GraphStreamingVerifyCC)
target_link_libraries(DistribUpdateStreamingVerifyCC PUBLIC GraphStreamingVerifyCC ${MPI_LIBRARIES})
target_include_directories(DistribUpdateStreamingVerifyCC PUBLIC include/ ${MPI_C_INCLUDE_PATH})
# TODO: Is MPI INCLUDE PATH necessary?
if(MPI_COMPILE_FLAGS)
  set_target_properties(DistribUpdateStreamingVerifyCC PROPERTIES
    COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
endif()
if(MPI_LINK_FLAGS)
  set_target_properties(DistribUpdateStreamingVerifyCC PROPERTIES
    LINK_FLAGS "${MPI_LINK_FLAGS}")
endif()

add_executable(distrib_tests
  test/distributed_graph_test.cpp
  test/test_runner.cpp
  ${graphstreamingcc_SOURCE_DIR}/test/util/graph_gen.cpp
  ${graphstreamingcc_SOURCE_DIR}/test/util/file_graph_verifier.cpp
)
add_dependencies(distrib_tests DistribUpdateStreamingVerifyCC)
target_link_libraries(distrib_tests PUBLIC DistribUpdateStreamingVerifyCC)

add_executable(speed_expr
  experiment/cluster_speed_expr.cpp
)
add_dependencies(speed_expr DistribUpdateStreamingCC)
target_link_libraries(speed_expr PUBLIC DistribUpdateStreamingCC)

add_executable(mpi_curve
  experiment/mpi_curve.cpp
)
target_link_libraries(mpi_curve PUBLIC ${MPI_LIBRARIES})

add_executable(binary_to_static
  tools/to_static.cpp
)
add_dependencies(binary_to_static GraphStreamingVerifyCC)
target_link_libraries(binary_to_static PUBLIC GraphStreamingVerifyCC)
