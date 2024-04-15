include(ExternalProject)
find_package(Git REQUIRED)

ExternalProject_Add(
        treeline_src
        PREFIX "vendor/treeline"
        GIT_REPOSITORY "https://github.com/zxjcarrot/treeline.git"
        TIMEOUT 10
        BUILD_COMMAND ""
        UPDATE_COMMAND "" # to prevent rebuilding everytime
        CMAKE_ARGS
        -DCMAKE_INSTALL_PREFIX=${CMAKE_BINARY_DIR}/vendor/treeline
        -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
        -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
)

ExternalProject_Get_Property(treeline_src source_dir)
ExternalProject_Get_Property(treeline_src binary_dir)
set(TREELINE_INCLUDE_DIR ${source_dir}/include)
set(TREELINE_LIBRARY_PATH ${binary_dir}/libtreeline.a)
set(PG_LIBRARY_PATH ${binary_dir}/page_grouping/libpg.a)
set(PG_TREELINE_LIBRARY_PATH ${binary_dir}/libpg_treeline.a)
set(MASSTREE_LIBRARY_PATH ${binary_dir}/third_party/masstree/libmasstree.a)
set(CRC32_LIBRARY_PATH ${binary_dir}/_deps/crc32c-build/libcrc32c.a)
file(MAKE_DIRECTORY ${TREELINE_INCLUDE_DIR})
add_library(treeline_core SHARED IMPORTED)
set_property(TARGET treeline_core PROPERTY IMPORTED_LOCATION ${TREELINE_LIBRARY_PATH})
add_library(pg_treeline_core SHARED IMPORTED)
set_property(TARGET pg_treeline_core PROPERTY IMPORTED_LOCATION ${PG_TREELINE_LIBRARY_PATH})
add_library(pg_core SHARED IMPORTED)
set_property(TARGET pg_core PROPERTY IMPORTED_LOCATION ${PG_LIBRARY_PATH})
add_library(treeline_crc32 SHARED IMPORTED)
set_property(TARGET treeline_crc32 PROPERTY IMPORTED_LOCATION ${CRC32_LIBRARY_PATH})
add_library(treeline_masstree SHARED IMPORTED)
set_property(TARGET treeline_masstree PROPERTY IMPORTED_LOCATION ${MASSTREE_LIBRARY_PATH})
add_library(treeline INTERFACE IMPORTED)
set_property(TARGET treeline PROPERTY
  INTERFACE_LINK_LIBRARIES pg_treeline_core pg_core treeline_crc32 treeline_masstree)
set_property(TARGET treeline APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${TREELINE_INCLUDE_DIR})
add_dependencies(treeline treeline_src)