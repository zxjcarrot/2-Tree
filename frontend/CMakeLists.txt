add_executable(frontend frontend.cpp)
add_dependencies(frontend leanstore)
target_link_libraries(frontend leanstore)


add_executable(ycsb_zipf ycsb/ycsb_zipf.cpp)
target_link_libraries(ycsb_zipf treeline leanstore Threads::Threads rocksdb tcmalloc)
target_include_directories(ycsb_zipf PRIVATE ${SHARED_INCLUDE_DIRECTORY} ${CMAKE_DL_LIBS})

#add_executable(ycsb_hotspot ycsb/ycsb_hotspot.cpp)
#target_link_libraries(ycsb_hotspot leanstore Threads::Threads)
#target_include_directories(ycsb_hotspot PRIVATE ${SHARED_INCLUDE_DIRECTORY} ${CMAKE_DL_LIBS})


# add_executable(btreeolc_test btreeolc_test/btreeolc_test.cpp)
# target_link_libraries(btreeolc_test leanstore gtest Threads::Threads)
# target_include_directories(btreeolc_test PRIVATE ${SHARED_INCLUDE_DIRECTORY} ${CMAKE_DL_LIBS})


add_executable(tpcc tpc-c/tpcc.cpp)
target_link_libraries(tpcc leanstore Threads::Threads tcmalloc)
target_include_directories(tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})

# # https://source.wiredtiger.com/10.0.0/build-posix.html
# add_executable(wiredtiger_tpcc tpc-c/wiredtiger_tpcc.cpp)
# target_link_libraries(wiredtiger_tpcc leanstore wiredtiger Threads::Threads dl z libbz2.a lz4 snappy)
# target_include_directories(wiredtiger_tpcc PRIVATE ${SHARED_INCLUDE_DIRECTORY})
