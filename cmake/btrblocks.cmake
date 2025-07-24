# ---------------------------------------------------------------------------
# btrblocks
# ---------------------------------------------------------------------------

include(ExternalProject)
find_package(Git REQUIRED)

# Get btrblocks
ExternalProject_Add(
        btrblocks_src
        PREFIX "vendor/btrblocks"
        GIT_REPOSITORY "https://github.com/pascalginter/btrblocks"
        GIT_TAG "arrow-support"
        CMAKE_ARGS
        -DBUILD_SHARED_LIBRARY=ON
        -DCMAKE_BUILD_TYPE=Release
        UPDATE_COMMAND git pull
)

ExternalProject_Get_Property(btrblocks_src install_dir)
set(BTRBLOCKS_LIBRARY_PATH ${install_dir}/src/btrblocks_src-build/libbtrblocks.so)
set(BTRBLOCKS_INCLUDE_DIR ${install_dir}/src/btrblocks_src/btrblocks)
set(BTRFILES_LIBRARY_PATH ${install_dir}/btrblocks_src-build/libbtrfiles.so)
set(BTRFILES_INCLUDE_DIR ${install_dir}/src/btrblocks_src/btrfiles)

file(MAKE_DIRECTORY ${BTRBLOCKS_INCLUDE_DIR})

add_library(btrblocks SHARED IMPORTED)
set_property(TARGET btrblocks PROPERTY IMPORTED_LOCATION ${BTRBLOCKS_LIBRARY_PATH})
set_property(TARGET btrblocks APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${BTRBLOCKS_INCLUDE_DIR})

add_library(btrfiles SHARED IMPORTED)
set_property(TARGET btrfiles PROPERTY IMPORTED_LOCATION ${BTRFILES_LIBRARY_PATH})
set_property(TARGET btrfiles APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${BTRFILES_INCLUDE_DIR})

# Dependencies
add_dependencies(btrblocks btrblocks_src)
add_dependencies(btrfiles btrblocks_src)

set(ARROW_PREFIX ${install_dir}/src/btrblocks_src-build/vendor/arrow)
include_directories(${ARROW_PREFIX}/include)
link_directories(${ARROW_PREFIX}/lib)