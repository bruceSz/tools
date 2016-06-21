SET(CMAKE_CXX_FLAGS "-std=c++11")
SET(kudu_manager_src
kudu_manager.cc
)
add_executable(kudu-manager ${kudu_manager_src})
