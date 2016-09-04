SET(CMAKE_CXX_FLAGS "-std=c++11 -g")
SET(COMMON_SRC util.cc)
SET(kudu_manager_src
kudu_manager.cc
${COMMON_SRC}
)

SET(csv_loader_src
csv_loader.cc
${COMMON_SRC}
)
add_executable(kudu-manager ${kudu_manager_src})
add_executable(csv_loader ${csv_loader_src})
target_link_libraries(kudu-manager kudu_client  gflags  )
target_link_libraries(csv_loader kudu_client gflags)
