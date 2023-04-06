add_executable(sqb sqb.cpp)
target_link_libraries(sqb TBB::tbb)
target_include_directories(sqb PRIVATE ..)
