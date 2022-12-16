cmake_minimum_required(VERSION 3.14)
project(blimp_db_cpu)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

find_package(Threads REQUIRED)

include(FetchContent)

FetchContent_Declare(
        tbb
        GIT_REPOSITORY https://github.com/oneapi-src/oneTBB.git
        GIT_TAG v2021.6.0
)
set(TBB_TEST OFF)
FetchContent_MakeAvailable(tbb)

FetchContent_Declare(
        duckdb
        GIT_REPOSITORY https://github.com/duckdb/duckdb.git
        GIT_TAG v0.6.1
)
FetchContent_MakeAvailable(duckdb)

add_executable(blimp_db_cpu src/run.cpp)
target_link_libraries(blimp_db_cpu duckdb TBB::tbb Threads::Threads)
