SET(CMAKE_CXX_FLAGS "-std=c++11 -g")
SET(kudu_manager_src
kudu_manager.cc
)
add_executable(kudu-manager ${kudu_manager_src})
target_link_libraries(kudu-manager kudu_client  gflags  )
