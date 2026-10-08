
set(LIBS
    ${LIBS}
    raft-kv++)

enable_testing()
find_package(GTest REQUIRED)


# 下面都是gtest单元测试的可执行文件
add_executable(test_raftlog test_raftlog.cpp)
target_link_libraries(test_raftlog ${LIBS})
gtest_add_tests(TARGET test_raftlog)

add_executable(test_progress test_progress.cpp)
target_link_libraries(test_progress ${LIBS})
gtest_add_tests(TARGET test_progress)

add_executable(test_raft test_raft.cpp network.hpp)
target_link_libraries(test_raft ${LIBS})
gtest_add_tests(TARGET test_raft)

add_executable(test_rawnode test_rawnode.cpp)
target_link_libraries(test_rawnode ${LIBS})
gtest_add_tests(TARGET test_rawnode)
