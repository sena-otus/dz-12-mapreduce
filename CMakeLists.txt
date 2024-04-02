cmake_minimum_required(VERSION 3.23)

set(PATCH_VERSION "1" CACHE INTERNAL "Patch version")
set(PROJECT_VERSION 0.0.${PATCH_VERSION})

# underscores are not allowed in deb package name, but adding underscore to comply with requirements...
project(mapreduce VERSION ${PROJECT_VERSION})

option(WITH_TEST "Whether to build Google test" ON)
option(WITH_CLANG_TIDY "use clang-tidy" OFF)

find_package(Boost REQUIRED COMPONENTS system filesystem)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(WITH_CLANG_TIDY)
  find_program(CLANGTIDY NAMES clang-tidy-15 clang-tidy-14 clang-tidy REQUIRED)
  set(CMAKE_CXX_CLANG_TIDY "${CLANGTIDY}")
  message(NOTICE "found and enabled CLANGTIDY=${CLANGTIDY}")
endif()

configure_file(version.h.in version.h)

add_executable(mapreduce main.cpp mapreduce.cpp mapfileloader.cpp)

target_link_libraries(mapreduce Boost::filesystem)

set_target_properties(mapreduce PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

target_include_directories(mapreduce
    PRIVATE "${CMAKE_BINARY_DIR}"
  )

if(WITH_TEST)
    find_package(GTest REQUIRED)
    enable_testing()

    add_executable(gtest_mapreduce gtest_mapreduce.cpp mapreduce.cpp mapfileloader.cpp)

    set_target_properties(gtest_mapreduce PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
    )

    target_link_libraries(gtest_mapreduce
      GTest::gtest_main
      Boost::filesystem
    )

    gtest_discover_tests(gtest_mapreduce)
endif()


if (MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /W4")
  else ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --all-warnings -Wall -Werror -Wextra -pedantic")
endif()

install(TARGETS mapreduce RUNTIME DESTINATION bin)

set(CPACK_GENERATOR DEB)
set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")
set(CPACK_PACKAGE_CONTACT example@example.com)
include(CPack)
