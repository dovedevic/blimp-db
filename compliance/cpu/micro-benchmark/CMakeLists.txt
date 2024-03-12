cmake_minimum_required(VERSION 3.14)
project(blimp_db_cpu)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")

include(FetchContent)

FetchContent_Declare(
        abseil
        GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
        GIT_TAG 20230802.1
)
set(ABSL_PROPAGATE_CXX_STD ON)
FetchContent_MakeAvailable(abseil)

FetchContent_Declare(
        cxxopts
        GIT_REPOSITORY https://github.com/jarro2783/cxxopts.git
        GIT_TAG v3.1.1
)
FetchContent_MakeAvailable(cxxopts)

FetchContent_Declare(
        tbb
        GIT_REPOSITORY https://github.com/oneapi-src/oneTBB.git
        GIT_TAG v2021.10.0
)
FetchContent_MakeAvailable(tbb)

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

add_executable(selection src/selection.cpp)
target_link_libraries(
        selection
        PRIVATE
        cxxopts::cxxopts
        TBB::tbb
)
