SET(CMAKE_CXX_FLAGS "-std=c++11 -g")
SET(COMMON_SRC util.cc)
SET(kudu_manager_src
kudu_manager.cc
${COMMON_SRC}
)

SET(csv_loader_src
csv_loader.cc
text_file.cc
${COMMON_SRC}
)

SET(queue_test_src
queue-test.cc
queue.cc
sem.cc
)

SET(text_file-test_src
text_file-test.cc
text_file.cc
)
SET(client-test_src
client-test.cc
util.cc
)

SET(util-test_src
util-test.cc
)

add_executable(kudu-manager ${kudu_manager_src})
add_executable(csv_loader ${csv_loader_src})
add_executable(queue-test ${queue_test_src})
add_executable(text_file-test ${text_file-test_src})
add_executable(util-test ${util-test_src})
add_executable(client-test ${client-test_src})
target_link_libraries(kudu-manager kudu_client  gflags  )
target_link_libraries(csv_loader kudu_client gflags)
target_link_libraries(text_file-test kudu_client)
target_link_libraries(client-test kudu_client)
target_link_libraries(queue-test pthread kudu_client)
