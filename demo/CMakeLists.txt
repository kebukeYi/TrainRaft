set(LIBS
    ${LIBS})
#   raft-kv++)

add_executable(demo_rocksdb demo_rocksdb.cpp)
target_link_libraries(demo_rocksdb ${LIBS})

