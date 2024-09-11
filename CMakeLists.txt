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

#add_compile_options(-fsanitize=address)
#add_link_options(-fsanitize=address)

######
# Get MPI for distributed communication
######
find_package(MPI REQUIRED)
include_directories(${MPI_INCLUDE_PATH})
add_definitions(-DOMPI_SKIP_MPICXX)
if(MPI_CXX_FOUND)
       message("Found MPI_CXX")
endif()

# set(FETCHCONTENT_UPDATES_DISCONNECTED)

# Install GraphZeppelin Project
FetchContent_Declare(
  GraphZeppelin

  GIT_REPOSITORY      https://github.com/GraphStreamingProject/GraphZeppelin
  GIT_TAG             v2.0.0
)
FetchContent_MakeAvailable(GraphZeppelin)
# Uncomment the next two lines to use CubeSketch algorithm
# target_compile_definitions(GraphZeppelin PUBLIC L0_SAMPLING)
# target_compile_definitions(GraphZeppelinVerifyCC PUBLIC L0_SAMPLING)

if (BUILD_BENCH)
  # Get Google Benchmark
  FetchContent_Declare(
      benchmark

      GIT_REPOSITORY https://github.com/google/benchmark
      GIT_TAG        v1.6.1
  )
  set(BENCHMARK_ENABLE_GTEST_TESTS OFF)

  FetchContent_MakeAvailable(benchmark)
endif()

# The library for distributing the CPU work for
# generating sketch deltas
add_library(DistribUpdateStreamingCC
  src/worker_cluster.cpp
  src/work_distributor.cpp
  src/distributed_worker.cpp
  src/message_forwarders.cpp
  src/graph_distrib_update.cpp
)
add_dependencies(DistribUpdateStreamingCC GraphZeppelin)
target_link_libraries(DistribUpdateStreamingCC PUBLIC GraphZeppelin ${MPI_LIBRARIES})
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
  src/message_forwarders.cpp
  src/graph_distrib_update.cpp
)
add_dependencies(DistribUpdateStreamingVerifyCC GraphZeppelinVerifyCC)
target_link_libraries(DistribUpdateStreamingVerifyCC PUBLIC GraphZeppelinVerifyCC ${MPI_LIBRARIES})
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
  test/k_connectivity_test.cpp
  test/test_runner.cpp
  ${GraphZeppelin_SOURCE_DIR}/test/util/graph_gen.cpp
  ${GraphZeppelin_SOURCE_DIR}/test/util/file_graph_verifier.cpp
)
add_dependencies(distrib_tests DistribUpdateStreamingVerifyCC)
target_link_libraries(distrib_tests PUBLIC DistribUpdateStreamingVerifyCC)

add_executable(speed_expr
  experiment/cluster_speed_expr.cpp
)
add_dependencies(speed_expr DistribUpdateStreamingCC)
target_link_libraries(speed_expr PUBLIC DistribUpdateStreamingCC)

add_executable(k_speed_expr
  experiment/cluster_k_connect_expr.cpp
)
add_dependencies(k_speed_expr DistribUpdateStreamingCC)
target_link_libraries(k_speed_expr PUBLIC DistribUpdateStreamingCC)

add_executable(query_expr
  experiment/cluster_query_expr.cpp  
)
add_dependencies(query_expr DistribUpdateStreamingCC)
target_link_libraries(query_expr PUBLIC DistribUpdateStreamingCC)

add_executable(correctness_expr
  experiment/cont_expr.cpp
)
add_dependencies(correctness_expr DistribUpdateStreamingVerifyCC)
target_link_libraries(correctness_expr PUBLIC DistribUpdateStreamingVerifyCC)

#####################################################################
#####################    Streaming utilities    #####################
#####################################################################
add_executable(binary_to_static
  tools/to_static.cpp
)
add_dependencies(binary_to_static GraphZeppelinVerifyCC)
target_link_libraries(binary_to_static PUBLIC GraphZeppelinVerifyCC)

#add_executable(stream_gen
#    tools/streaming/hash_streamer.cpp
#    tools/streaming/gz_specific/gz_nonsequential_streamer.cpp
#    tools/streaming/gz_specific/gz_sequential_streamer.cpp
#    tools/streaming/stream_finder.cpp
#)
#add_dependencies(stream_gen GraphZeppelin)
#target_link_libraries(stream_gen PUBLIC GraphZeppelin)

add_executable(mpi_experiment
  experiment/mpi_message_throughput.cpp
)
target_link_libraries(mpi_experiment PUBLIC ${MPI_LIBRARIES})
if(MPI_COMPILE_FLAGS)
  set_target_properties(mpi_experiment PROPERTIES
    COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
endif()
if(MPI_LINK_FLAGS)
  set_target_properties(mpi_experiment PROPERTIES
    LINK_FLAGS "${MPI_LINK_FLAGS}")
endif()

if (BUILD_BENCH)
  add_executable(bench_streaming
      tools/streaming/streamer_bench.cpp
#      tools/streaming/hash_streamer.cpp
#      tools/streaming/gz_specific/gz_nonsequential_streamer.cpp
      tools/streaming/gz_specific/gz_sequential_streamer.cpp
      )
  add_dependencies(bench_streaming GraphZeppelin benchmark)
  target_link_libraries(bench_streaming GraphZeppelin benchmark::benchmark)
endif()
