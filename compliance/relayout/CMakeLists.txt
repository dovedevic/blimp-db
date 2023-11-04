cmake_minimum_required(VERSION 3.4)

project(blimp_db_relayout)

set(CMAKE_CXX_STANDARD 17)

include(FetchContent)

FetchContent_Declare(
        tbb
        GIT_REPOSITORY https://github.com/oneapi-src/oneTBB.git
        GIT_TAG v2021.10.0
)
FetchContent_MakeAvailable(tbb)

add_executable(efficient_from_rank_v1 efficient_from_rank_v1.cpp)
add_executable(efficient_from_rank_v2 efficient_from_rank_v2.cpp)

target_link_libraries(efficient_from_rank_v2 TBB::tbb)
