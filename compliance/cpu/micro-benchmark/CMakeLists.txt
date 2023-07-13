cmake_minimum_required(VERSION 3.14)
project(blimp_db_cpu)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

find_package(absl CONFIG REQUIRED)
find_package(cxxopts CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)

add_executable(run src/run.cpp)
target_link_libraries(
        run
        PRIVATE
        absl::base
        absl::flat_hash_set
        absl::flat_hash_map
        cxxopts::cxxopts
        TBB::tbb
)
